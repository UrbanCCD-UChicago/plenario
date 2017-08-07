import codecs
import csv
import io
import json
import re

import shapely.geometry
import shapely.wkb
import sqlalchemy

from collections import OrderedDict
from dateutil import parser
from flask import Response, jsonify, request, stream_with_context

from plenario.api.common import CACHE_TIMEOUT, cache, crossdomain, make_cache_key, unknown_object_json_handler
from plenario.api.condition_builder import parse_tree
from plenario.api.jobs import get_job, make_job_response
from plenario.api.validator import DatasetRequiredValidator, NoDefaultDatesValidator, \
    NoGeoJSONDatasetRequiredValidator, NoGeoJSONValidator, has_tree_filters, validate, \
    PointsetRequiredValidator
from plenario.server import db
from plenario.models import MetaTable
from . import response as api_response


# ======
# routes
# ======

# The get_job method in jobs.py does not have crossdomain, and is out of the
# Flask context. So we define a wrapper here to access it.
@crossdomain(origin='*')
def get_job_view(ticket):
    return jsonify(get_job(ticket))


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin='*')
def detail_aggregate():
    fields = ('location_geom__within', 'dataset_name', 'agg', 'obs_date__ge',
              'obs_date__le', 'data_type', 'job')
    validator = NoGeoJSONDatasetRequiredValidator(only=fields)
    validator_result = validate(validator, request.args.to_dict())

    if validator_result.errors:
        return api_response.bad_request(validator_result.errors)

    time_counts = _detail_aggregate(validator_result)
    db.session.commit()
    return api_response.detail_aggregate_response(time_counts, validator_result)


@crossdomain(origin='*')
def datadump_view():
    fields = ('location_geom__within', 'dataset_name', 'shape', 'obs_date__ge',
              'obs_date__le', 'offset', 'date__time_of_day_ge',
              'date__time_of_day_le', 'limit', 'job', 'data_type')

    validator = DatasetRequiredValidator(only=fields)
    validator_result = validate(validator, request.args.to_dict())

    if validator_result.errors:
        return api_response.error(validator_result.errors, 400)

    stream = datadump(**validator_result.data)

    dataset = validator_result.data['dataset'].name
    fmt = validator_result.data['data_type']
    content_disposition = 'attachment; filename={}.{}'.format(dataset, fmt)

    attachment = Response(stream_with_context(stream), mimetype='text/%s' % fmt)
    attachment.headers['Content-Disposition'] = content_disposition
    return attachment


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin='*')
def grid():

    fields = (
        'dataset',
        'dataset_name',
        'resolution',
        'buffer',
        'obs_date__le',
        'obs_date__ge',
        'location_geom__within',
    )

    validator = PointsetRequiredValidator(only=fields)
    validator_result = validate(validator, request.args)
    if validator_result.errors:
        return api_response.bad_request(validator_result.errors)

    results = _grid(validator_result)

    query = validator.dumps(validator_result.data)
    query = json.loads(query.data)
    results['properties'] = query
    return jsonify(results)


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin='*')
def dataset_fields(dataset_name):
    request_args = request.args.to_dict()
    request_args['dataset_name'] = dataset_name
    fields = ('obs_date__le', 'obs_date__ge', 'dataset_name', 'job')
    validator = DatasetRequiredValidator(only=fields)
    validator_result = validate(validator, request_args)

    if validator_result.errors:
        return api_response.bad_request(validator_result.errors)

    if validator_result.data.get('job'):
        return make_job_response('fields', validator_result)
    else:
        result_data = _meta(validator_result)
        return api_response.fields_response(result_data, validator_result)


# ============
# _route logic
# ============


def _detail_aggregate(args):
    """Returns a record for every row in the specified dataset with brief
    temporal and spatial information about the row. This can give a user of the
    platform a quick overview about what is available within their constraints.

    :param args: dictionary of request arguments
    :returns: csv or json response object
    """
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
        table = MetaTable.get_by_dataset_name(tablename).table
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
            return api_response.make_raw_error('{}: {}'.format(msg, e))

        time_counts += [{'count': c, 'datetime': d} for c, d in ts[1:]]

    return time_counts


def datadump(**kwargs):
    """Export the result of a detail query in geojson or csv format. Returns a
    generator that yields pieces of the export.
    """
    if kwargs.get('data_type') == 'json':
        return datadump_json(**kwargs)
    return datadump_csv(**kwargs)


def datadump_json(**kwargs):
    """Export the result of a detail query as valid geojson, where each row is
    formatted as a feature with its column-value pairs stored in the properties
    field. Plenario derived columns are hidden.
    """
    class ValidatorResultProxy(object):
        pass

    vr_proxy = ValidatorResultProxy()
    vr_proxy.data = kwargs

    dataset = kwargs['dataset']
    columns = [c.name for c in dataset.c]
    query = detail_query(vr_proxy)

    buffer = ''
    chunksize = 1000

    yield "{'type': 'FeatureCollection', 'features': ["

    for i, row in enumerate(query.yield_per(chunksize)):
        wkb = row.geom

        try:
            geom = shapely.wkb.loads(wkb.desc, hex=True).__geo_interface__
        except AttributeError:
            continue

        geojson = {
            'type': 'Feature',
            'geometry': geom,
            'properties': dict(zip(columns, row))
        }
        del geojson['properties']['geom']
        del geojson['properties']['hash']

        buffer += json.dumps(geojson, default=unknown_object_json_handler)
        buffer += ','

        if i % chunksize == 0:
            yield buffer
            buffer = ''

    # Remove the trailing comma and close the json
    yield buffer.rsplit(',', 1)[0] + ']}'


def datadump_csv(**kwargs):
    """Export the result of a detail query as a comma-delimited csv file. The
    header row is taken directly from the table's column list, with Plenario
    derived values hidden.
    """
    class ValidatorResultProxy(object):
        pass

    vr_proxy = ValidatorResultProxy()
    vr_proxy.data = kwargs

    dataset = kwargs['dataset']
    query = detail_query(vr_proxy)

    rownum = 0
    chunksize = 1000
    hide = {'geom', 'hash'}

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
        return api_response.bad_request('Too many table filters provided.')

    # Query the point dataset.
    q = db.session.query(dataset)

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
    dataset_conditions = {k: v for k, v in args.data.items() if 'filter' in k}
    for tablename, condition_tree in dataset_conditions.items():

        tablename = tablename.rsplit('__')[0]

        metatable = MetaTable.get_by_dataset_name(tablename)
        table = metatable.table
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
            return api_response.make_raw_error('{}: {}'.format(msg, e))

    resp = api_response.geojson_response_base()
    for value in result_rows:
        if value[1]:
            pt = shapely.wkb.loads(codecs.decode(value[1], 'hex'))
            south, west = (pt.x - (size_x / 2)), (pt.y - (size_y / 2))
            north, east = (pt.x + (size_x / 2)), (pt.y + (size_y / 2))
            new_geom = shapely.geometry.box(south, west, north, east).__geo_interface__
        else:
            new_geom = None
        new_property = {'count': value[0], }
        api_response.add_geojson_feature(resp, new_geom, new_property)

    return resp


# =====
# Utils
# =====

def request_args_to_condition_tree(request_args, ignore=list()):
    """Take dictionary that has a 'dataset' key and column arguments into
    a single and build a condition tree.

    :param request_args: dictionary with a dataset and column arguments
    :param ignore: what values to not use for building conditions
    :returns: condition tree
    """
    ignored = {'agg', 'data_type', 'dataset', 'geom', 'limit', 'offset',
               'shape', 'shapeset', 'job', 'all', 'datadump_part', 'datadump_total',
               'datadump_requestid', 'datadump_urlroot', 'jobsframework_ticket', 'jobsframework_workerid',
               'jobsframework_workerbirthtime'}
    for val in ignore:
        ignored.add(val)

    # If the key wasn't convertable, it meant that it was a column key.
    columns = {k: v for k, v in list(request_args.items()) if k not in ignored}

    ctree = {'op': 'and', 'val': []}

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
            ctree['val'].append({'op': 'eq', 'col': k[0], 'val': v})
        elif len(k) == 2:
            ctree['val'].append({'op': k[1], 'col': k[0], 'val': v})

    return ctree
