import codecs
import csv
import io
import json
import re
import traceback
from collections import OrderedDict

import shapely.geometry
import shapely.wkb
import sqlalchemy
from dateutil import parser
from flask import request, Response, jsonify, stream_with_context

from plenario.api.common import cache, crossdomain, CACHE_TIMEOUT
from plenario.api.common import make_cache_key, unknown_object_json_handler
from plenario.api.condition_builder import parse_tree
from plenario.api.jobs import make_job_response, get_job
from plenario.api.validator import DatasetRequiredValidator
from plenario.api.validator import NoDefaultDatesValidator, NoGeoJSONValidator
from plenario.api.validator import NoGeoJSONDatasetRequiredValidator
from plenario.api.validator import validate, has_tree_filters
from plenario.database import postgres_session
from plenario.models import MetaTable
from . import response as api_response


# ======
# routes
# ======

# The get_job method in jobs.py does not have crossdomain, and is out of the
# Flask context. So we define a wrapper here to access it.
@crossdomain(origin="*")
def get_job_view(ticket):
    return jsonify(get_job(ticket))


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def timeseries():
    fields = ('location_geom__within', 'dataset_name', 'dataset_name__in',
              'agg', 'obs_date__ge', 'obs_date__le', 'data_type', 'job')
    validator = NoGeoJSONValidator(only=fields)
    validator_result = validate(validator, request.args.to_dict())

    if validator_result.errors:
        return api_response.bad_request(validator_result.errors)

    if validator_result.data.get('job'):
        return make_job_response("timeseries", validator_result)
    else:
        panel = _timeseries(validator_result)
        return api_response.timeseries_response(panel, validator_result)


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def detail_aggregate():
    fields = ('location_geom__within', 'dataset_name', 'agg', 'obs_date__ge',
              'obs_date__le', 'data_type', 'job')
    validator = NoGeoJSONDatasetRequiredValidator(only=fields)
    validator_result = validate(validator, request.args.to_dict())

    if validator_result.errors:
        return api_response.bad_request(validator_result.errors)

    if validator_result.data.get('job'):
        return make_job_response("detail-aggregate", validator_result)
    else:
        time_counts = _detail_aggregate(validator_result)
        return api_response.detail_aggregate_response(time_counts, validator_result)


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def detail():
    fields = ('location_geom__within', 'dataset_name', 'shape', 'obs_date__ge',
              'obs_date__le', 'data_type', 'offset', 'date__time_of_day_ge',
              'date__time_of_day_le', 'limit', 'job')
    validator = DatasetRequiredValidator(only=fields)
    validator_result = validate(validator, request.args.to_dict())

    if validator_result.errors:
        return api_response.bad_request(validator_result.errors)

    if validator_result.data.get('job'):
        return make_job_response("detail", validator_result)
    else:
        result_rows = _detail(validator_result)
        return api_response.detail_response(result_rows, validator_result)


@crossdomain(origin="*")
def datadump_view():

    fields = ('location_geom__within', 'dataset_name', 'shape', 'obs_date__ge',
              'obs_date__le', 'offset', 'date__time_of_day_ge',
              'date__time_of_day_le', 'limit', 'job', 'data_type')

    validator = DatasetRequiredValidator(only=fields)
    validator_result = validate(validator, request.args.to_dict())

    if validator_result.errors:
        return api_response.error(validator_result.errors, 400)

    stream = datadump(**validator_result.data)

    dataset = validator_result.data["dataset"].name
    fmt = validator_result.data["data_type"]
    content_disposition = 'attachment; filename={}.{}'.format(dataset, fmt)

    attachment = Response(stream_with_context(stream), mimetype="text/%s" % fmt)
    attachment.headers["Content-Disposition"] = content_disposition
    return attachment


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def grid():
    fields = ('dataset_name', 'resolution', 'buffer', 'obs_date__le', 'obs_date__ge',
              'location_geom__within', 'job')
    validator_result = validate(DatasetRequiredValidator(only=fields), request.args.to_dict())

    if validator_result.errors:
        return api_response.bad_request(validator_result.errors)

    if validator_result.data.get('job'):
        return make_job_response("grid", validator_result)
    else:
        result_data = _grid(validator_result)
        return api_response.grid_response(result_data)


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def dataset_fields(dataset_name):
    request_args = request.args.to_dict()
    request_args['dataset_name'] = dataset_name
    fields = ('obs_date__le', 'obs_date__ge', 'dataset_name', 'job')
    validator = DatasetRequiredValidator(only=fields)
    validator_result = validate(validator, request_args)

    if validator_result.errors:
        return api_response.bad_request(validator_result.errors)

    if validator_result.data.get('job'):
        return make_job_response("fields", validator_result)
    else:
        result_data = _meta(validator_result)
        return api_response.fields_response(result_data, validator_result)


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def meta():
    fields = ('obs_date__le', 'obs_date__ge', 'dataset_name', 'location_geom__within', 'job')
    validator_result = validate(NoDefaultDatesValidator(only=fields), request.args.to_dict())

    if validator_result.errors:
        return api_response.bad_request(validator_result.errors)

    if validator_result.data.get('job'):
        return make_job_response("meta", validator_result)
    else:
        result_data = _meta(validator_result)
        return api_response.meta_response(result_data, validator_result)


# ============
# _route logic
# ============


def _timeseries(args):
    meta_params = ['geom', 'dataset', 'dataset_name__in', 'obs_date__ge', 'obs_date__le', 'agg']
    meta_vals = [args.data.get(k) for k in meta_params]
    geom, dataset, table_names, start_date, end_date, agg = meta_vals

    ctrees = {}

    if has_tree_filters(args.data):
        # Timeseries is a little tricky. If there aren't filters,
        # it would be ridiculous to build a condition tree for every one.
        for field, value in list(args.data.items()):
            if 'filter' in field:
                # This pattern matches the last occurrence of the '__' pattern.
                # Prevents an error that is caused by dataset names with trailing
                # underscores.
                tablename = re.split(r'__(?!_)', field)[0]
                metarecord = MetaTable.get_by_dataset_name(tablename)
                pt = metarecord.point_table
                ctrees[pt.name] = parse_tree(pt, value)
        # Just cleanliness, since we don't use this argument. Doesn't have
        # to show up in the JSON response.
        del args.data['dataset']

    # If no dataset_name__in list was provided, have to fill it in by invoking
    # MetaTable.index() here! Not in the validator. This way the list stays up
    # to date.
    if table_names is None:
        table_names = MetaTable.index()
        args.data['dataset_name__in'] = table_names

    # If a single dataset was provided, it's the only thing we need to consider.
    if dataset is not None:
        table_names = [dataset.name]
        del args.data['dataset_name__in']

    # remove table names which wouldn't return anything for the query, given
    # the time and geom constraints
    try:
        table_names = MetaTable.narrow_candidates(table_names, start_date, end_date, geom)
    except Exception as e:
        traceback.print_exc()
        msg = 'Failed to gather candidate tables.'
        return api_response.make_raw_error("{}: {}".format(msg, e))
        # TODO: Correctly handle _timeseries (and all the other endpoints)
        # TODO: so that make_error is called when there is an error.

    # If there aren't any table names, it causes an error down the code. Better
    # to return and inform them that the request wouldn't have found anything.
    if not table_names:
        return api_response.bad_request("Your request doesn't return any results. Try "
                                        "adjusting your time constraint or location "
                                        "parameters.")

    try:
        panel = MetaTable.timeseries_all(
            table_names, agg, start_date, end_date, geom, ctrees
        )
    except Exception as e:
        msg = 'Failed to construct timeseries.'
        return api_response.make_raw_error("{}: {}".format(msg, e))

    return MetaTable.attach_metadata(panel)


def _detail_aggregate(args):
    """Returns a record for every row in the specified dataset with brief
    temporal and spatial information about the row. This can give a user of the
    platform a quick overview about what is available within their constraints.

    :param args: dictionary of request arguments

    :returns: csv or json response object"""

    meta_params = ('obs_date__ge', 'obs_date__le', 'agg', 'geom', 'dataset')
    meta_vals = (args.data.get(k) for k in meta_params)
    start_date, end_date, agg, geom, dataset = meta_vals

    time_counts = []

    if not has_tree_filters(args.data):
        # The obs_date arguments set the bounds of all the aggregates.
        # We don't want to create a condition tree that has point_date filters.
        args.data[dataset.name + '__filter'] = request_args_to_condition_tree(
            args.data, ignore=['obs_date__ge', 'obs_date__le']
        )

    dataset_conditions = {k: v for k, v in list(args.data.items()) if 'filter' in k}
    for tablename, condition_tree in list(dataset_conditions.items()):
        # This pattern matches the last occurrence of the '__' pattern.
        # Prevents an error that is caused by dataset names with trailing
        # underscores.
        tablename = re.split(r'__(?!_)', tablename)[0]
        table = MetaTable.get_by_dataset_name(tablename).point_table
        try:
            conditions = parse_tree(table, condition_tree)
        except ValueError:  # Catches empty condition tree.
            conditions = None

        try:
            ts = MetaTable.get_by_dataset_name(table.name).timeseries_one(
                agg, start_date, end_date, geom, conditions
            )
        except Exception as e:
            msg = 'Failed to construct timeseries'
            return api_response.make_raw_error("{}: {}".format(msg, e))

        time_counts += [{'count': c, 'datetime': d} for c, d in ts[1:]]

    return time_counts


def _detail(args):
    meta_params = ('dataset', 'shape', 'data_type', 'limit', 'offset')
    meta_vals = (args.data.get(k) for k in meta_params)
    dataset, shapeset, data_type, limit, offset = meta_vals

    q = detail_query(args).order_by(dataset.c.point_date.desc())

    # Apply limit and offset.
    q = q.limit(limit)
    q = q.offset(offset) if offset else q

    try:
        columns = [c.name for c in dataset.columns]
        if shapeset:
            columns += [c.name for c in shapeset.columns]
        return [OrderedDict(list(zip(columns, row))) for row in q.all()]
    except Exception as e:
        postgres_session.rollback()
        msg = "Failed to fetch records."
        return api_response.make_raw_error("{}: {}".format(msg, e))


def datadump(**kwargs):
    """Export the result of a detail query in geojson or csv format. Returns a
    generator that yields pieces of the export."""

    if kwargs.get("data_type") == "json":
        return datadump_json(**kwargs)
    return datadump_csv(**kwargs)


def datadump_json(**kwargs):
    """Export the result of a detail query as valid geojson, where each row is
    formatted as a feature with its column-value pairs stored in the properties
    field. Plenario derived columns are hidden."""

    class ValidatorResultProxy(object):
        pass
    vr_proxy = ValidatorResultProxy()
    vr_proxy.data = kwargs

    dataset = kwargs["dataset"]
    columns = [c.name for c in dataset.c]
    query = detail_query(vr_proxy)

    buffer = ""
    chunksize = 1000

    yield '{"type": "FeatureCollection", "features": ['

    for i, row in enumerate(query.yield_per(chunksize)):
        wkb = row.geom

        try:
            geom = shapely.wkb.loads(wkb.desc, hex=True).__geo_interface__
        except AttributeError:
            continue

        geojson = {
            "type": "Feature",
            "geometry": geom,
            "properties": dict(zip(columns, row))
        }
        del geojson["properties"]["geom"]
        del geojson["properties"]["hash"]

        buffer += json.dumps(geojson, default=unknown_object_json_handler)
        buffer += ","

        if i % chunksize == 0:
            yield buffer
            buffer = ""

    # Remove the trailing comma and close the json
    yield buffer.rsplit(',', 1)[0] + "]}"


def datadump_csv(**kwargs):
    """Export the result of a detail query as a comma-delimited csv file. The
    header row is taken directly from the table's column list, with Plenario
    derived values hidden."""

    class ValidatorResultProxy(object):
        pass
    vr_proxy = ValidatorResultProxy()
    vr_proxy.data = kwargs

    dataset = kwargs["dataset"]
    query = detail_query(vr_proxy)

    rownum = 0
    chunksize = 1000
    hide = {"geom", "hash"}

    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow([c.name for c in dataset.c if c not in hide])

    for row in query.yield_per(chunksize):
        rownum += 1
        writer.writerow([getattr(row, c) for c in row.keys() if c not in hide])

        if rownum % chunksize == 0:
            yield buffer.getvalue()
            buffer.close()
            buffer = io.StringIO()
            writer = csv.writer(buffer)

    yield buffer.getvalue()
    buffer.close()


def detail_query(args, aggregate=False):
    meta_params = ('dataset', 'shapeset', 'data_type', 'geom', 'obs_date__ge',
                   'obs_date__le')
    meta_vals = (args.data.get(k) for k in meta_params)
    dataset, shapeset, data_type, geom, obs_date__ge, obs_date__le = meta_vals

    # If there aren't tree filters provided, a little formatting is needed
    # to make the general filters into an 'and' tree.
    if not has_tree_filters(args.data):
        # Creates an AND condition tree and adds it to args.
        args.data[dataset.name + '__filter'] = request_args_to_condition_tree(
            request_args=args.data,
            ignore=['shapeset']
        )

    # Sort out the filter conditions from the rest of the user arguments.
    filters = {k: v for k, v in list(args.data.items()) if 'filter' in k}

    # Get upset if they specify more than a dataset and shapeset filter.
    if len(filters) > 2:
        return api_response.bad_request("Too many table filters provided.")

    # Query the point dataset.
    q = postgres_session.query(dataset)

    # If the user specified a geom, filter results to those within its shape.
    if geom:
        q = q.filter(dataset.c.geom.ST_Within(
            sqlalchemy.func.ST_GeomFromGeoJSON(geom)
        ))

    # Retrieve the filters and build conditions from them if they exist.
    point_ctree = filters.get(dataset.name + '__filter')

    # If the user specified point dataset filters, parse and apply them.
    if point_ctree:
        point_conditions = parse_tree(dataset, point_ctree)
        q = q.filter(point_conditions)

        # To allow both obs_date meta params and filter trees.
        q = q.filter(dataset.c.point_date >= obs_date__ge) if obs_date__ge else q
        q = q.filter(dataset.c.point_date <= obs_date__le) if obs_date__le else q

    # If a user specified a shape dataset, it was either through the /shapes
    # enpoint, which uses the aggregate result, or through the /detail endpoint
    # which uses the joined result.
    if shapeset is not None:
        if aggregate:
            q = q.from_self(shapeset).filter(dataset.c.geom.ST_Intersects(shapeset.c.geom)).group_by(shapeset)
        else:
            shape_columns = ['{}.{} as {}'.format(shapeset.name, col.name, col.name) for col in shapeset.c]
            q = q.join(shapeset, dataset.c.geom.ST_Within(shapeset.c.geom))
            q = q.add_columns(*shape_columns)

        # If there's a filter specified for the shape dataset, apply those conditions.
        shape_ctree = filters.get(shapeset.name + '__filter')
        if shape_ctree:
            shape_conditions = parse_tree(shapeset, shape_ctree)
            q = q.filter(shape_conditions)

    return q


def _grid(args):
    meta_params = ('dataset', 'geom', 'resolution', 'buffer', 'obs_date__ge',
                   'obs_date__le')
    meta_vals = (args.data.get(k) for k in meta_params)
    point_table, geom, resolution, buffer_, obs_date__ge, obs_date__le = meta_vals

    result_rows = []

    if not has_tree_filters(args.data):
        tname = point_table.name
        args.data[tname + '__filter'] = request_args_to_condition_tree(
            request_args=args.data,
            ignore=['buffer', 'resolution']
        )

    # We only build conditions from values with a key containing 'filter'.
    # Therefore we only build dataset conditions from condition trees.
    dataset_conditions = {k: v for k, v in list(args.data.items()) if 'filter' in k}
    for tablename, condition_tree in list(dataset_conditions.items()):

        tablename = tablename.split('__')[0]

        metatable = MetaTable.get_by_dataset_name(tablename)
        table = metatable.point_table
        conditions = parse_tree(table, condition_tree)

        try:
            registry_row = MetaTable.get_by_dataset_name(table.name)
            # make_grid expects conditions to be iterable.
            grid_rows, size_x, size_y = registry_row.make_grid(
                resolution,
                geom,
                [conditions],
                {'upper': obs_date__le, 'lower': obs_date__ge}
            )
            result_rows += grid_rows
        except Exception as e:
            msg = 'Could not make grid aggregation.'
            return api_response.make_raw_error("{}: {}".format(msg, e))

    resp = api_response.geojson_response_base()
    for value in result_rows:
        if value[1]:
            pt = shapely.wkb.loads(codecs.decode(value[1], "hex"))
            south, west = (pt.x - (size_x / 2)), (pt.y - (size_y / 2))
            north, east = (pt.x + (size_x / 2)), (pt.y + (size_y / 2))
            new_geom = shapely.geometry.box(south, west, north, east).__geo_interface__
        else:
            new_geom = None
        new_property = {'count': value[0],}
        api_response.add_geojson_feature(resp, new_geom, new_property)

    return resp


def _meta(args):
    """Generate meta information about table(s) with records from MetaTable.

    :param args: dictionary of request arguments (?foo=bar)

    :returns: response dictionary"""

    meta_params = ('dataset', 'geom', 'obs_date__ge', 'obs_date__le')
    meta_vals = (args.data.get(k) for k in meta_params)
    dataset, geom, start_date, end_date = meta_vals

    # Columns to select as-is
    cols_to_return = ['human_name', 'dataset_name', 'source_url', 'view_url',
                      'date_added', 'last_update', 'update_freq', 'attribution',
                      'description', 'obs_from', 'obs_to', 'column_names',
                      'observed_date', 'latitude', 'longitude', 'location']
    col_objects = [getattr(MetaTable, col) for col in cols_to_return]

    # Columns that need pre-processing
    col_objects.append(sqlalchemy.func.ST_AsGeoJSON(MetaTable.bbox))
    cols_to_return.append('bbox')

    # Only return datasets that have been successfully ingested
    q = postgres_session.query(*col_objects).filter(MetaTable.date_added.isnot(None))

    # Filter over datasets if user provides full date range or geom
    should_filter = geom or (start_date and end_date)

    if dataset is not None:
        # If the user specified a name, don't try any filtering.
        # Just spit back that dataset's metadata.
        q = q.filter(MetaTable.dataset_name == dataset.name)

    # Otherwise, just send back all the (filtered) datasets
    elif should_filter:
        if geom:
            intersects = sqlalchemy.func.ST_Intersects(
                sqlalchemy.func.ST_GeomFromGeoJSON(geom),
                MetaTable.bbox
            )
            q = q.filter(intersects)
        if start_date and end_date:
            q = q.filter(
                sqlalchemy.and_(
                    MetaTable.obs_from < end_date,
                    MetaTable.obs_to > start_date
                )
            )

    metadata_records = [dict(list(zip(cols_to_return, row))) for row in q.all()]
    for record in metadata_records:
        try:
            if record.get('bbox') is not None:
                # serialize bounding box geometry to string
                record['bbox'] = json.loads(record['bbox'])
            # format columns in the expected way
            record['columns'] = [{'field_name': k, 'field_type': v}
                                 for k, v in list(record['column_names'].items())]
        except Exception as e:
            args.warnings.append(e.message)

        # clear column_names off the json, users don't need to see it
        del record['column_names']

    return metadata_records


# =====
# Utils
# =====

def request_args_to_condition_tree(request_args, ignore=list()):
    """Take dictionary that has a 'dataset' key and column arguments into
    a single and build a condition tree.

    :param request_args: dictionary with a dataset and column arguments
    :param ignore: what values to not use for building conditions

    :returns: condition tree"""

    ignored = {'agg', 'data_type', 'dataset', 'geom', 'limit', 'offset',
               'shape', 'shapeset', 'job', 'all', 'datadump_part', 'datadump_total',
               "datadump_requestid", "datadump_urlroot", "jobsframework_ticket", "jobsframework_workerid",
               "jobsframework_workerbirthtime"}
    for val in ignore:
        ignored.add(val)

    # If the key wasn't convertable, it meant that it was a column key.
    columns = {k: v for k, v in list(request_args.items()) if k not in ignored}

    ctree = {"op": "and", "val": []}

    # Add AND conditions based on query string parameters.
    for k, v in list(columns.items()):
        k = k.split('__')
        if k[0] == 'obs_date':
            k[0] = 'point_date'
        if k[0] == 'date' and 'time_of_day' in k[1]:
            k[0] = sqlalchemy.func.date_part('hour', request_args.get('dataset').c.point_date)
            k[1] = 'le' if 'le' in k[1] else 'ge'

        # It made me nervous that you could pass the parser in the validator
        # with values like 2000; or 2000' (because the parser strips them).
        # Before letting those values get passed to psycopg, I'm
        # just going to convert those values to datetimes here.
        elif 'date' in k[0]:
            try:
                v = parser.parse(v)
            except (AttributeError, TypeError):
                pass

        if len(k) == 1:
            ctree['val'].append({"op": "eq", "col": k[0], "val": v})
        elif len(k) == 2:
            ctree['val'].append({"op": k[1], "col": k[0], "val": v})

    return ctree
