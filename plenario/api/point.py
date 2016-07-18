import json
import shapely.geometry
import shapely.wkb
import sqlalchemy

from collections import OrderedDict
from datetime import datetime
from flask import request, make_response
from itertools import groupby
from operator import itemgetter

from plenario.api.common import cache, crossdomain, CACHE_TIMEOUT
from plenario.api.common import make_cache_key, date_json_handler, unknown_object_json_handler
from plenario.api.condition_builder import parse_tree
from plenario.api.response import internal_error, bad_request, json_response_base, make_csv
from plenario.api.response import geojson_response_base, form_csv_detail_response, form_json_detail_response
from plenario.api.response import form_geojson_detail_response, add_geojson_feature
from plenario.api.validator import DatasetRequiredValidator, NoGeoJSONDatasetRequiredValidator
from plenario.api.validator import NoDefaultDatesValidator, validate, NoGeoJSONValidator, has_tree_filters
from plenario.database import session
from plenario.models import MetaTable


# ======
# routes
# ======

@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def timeseries():
    fields = ('location_geom__within', 'dataset_name', 'dataset_name__in',
              'agg', 'obs_date__ge', 'obs_date__le', 'data_type')
    validator = NoGeoJSONValidator(only=fields)
    validated_args = validate(validator, request.args.to_dict())
    if validated_args.errors:
        return bad_request(validated_args.errors)

    return _timeseries(validated_args)


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def detail_aggregate():
    fields = ('location_geom__within', 'dataset_name', 'agg', 'obs_date__ge',
              'obs_date__le', 'data_type')
    validator = NoGeoJSONDatasetRequiredValidator(only=fields)
    validated_args = validate(validator, request.args.to_dict())
    if validated_args.errors:
        return bad_request(validated_args.errors)
    return _detail_aggregate(validated_args)


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def detail():
    fields = ('location_geom__within', 'dataset_name', 'shape', 'obs_date__ge',
              'obs_date__le', 'data_type', 'offset', 'date__time_of_day_ge',
              'date__time_of_day_le', 'limit')
    validator = DatasetRequiredValidator(only=fields)
    validated_args = validate(validator, request.args.to_dict())
    if validated_args.errors:
        return bad_request(validated_args.errors)

    return _detail(validated_args)


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def grid():
    fields = ('dataset_name', 'resolution', 'buffer', 'obs_date__le', 'obs_date__ge',
              'location_geom__within')
    validated_args = validate(DatasetRequiredValidator(only=fields), request.args.to_dict())
    if validated_args.errors:
        return bad_request(validated_args.errors)

    return _grid(validated_args)


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def dataset_fields(dataset_name):
    request_args = request.args.to_dict()
    request_args['dataset_name'] = dataset_name
    fields = ('obs_date__le', 'obs_date__ge', 'dataset_name')
    validator = DatasetRequiredValidator(only=fields)
    validated_args = validate(validator, request_args)
    if validated_args.errors:
        return bad_request(validated_args.errors)

    response = _meta(validated_args)

    # API defines column values to be in the 'objects' list.
    resp_dict = json.loads(response.data)
    resp_dict['objects'] = resp_dict['objects'][0]['columns']
    response.data = json.dumps(resp_dict)
    return response


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def meta():
    fields = ('obs_date__le', 'obs_date__ge', 'dataset_name', 'location_geom__within')
    validated_args = validate(NoDefaultDatesValidator(only=fields), request.args.to_dict())
    if validated_args.errors:
        return bad_request(validated_args.errors)

    return _meta(validated_args)


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
        for field, value in args.data.items():
            if 'filter' in field:
                metarecord = MetaTable.get_by_dataset_name(field.split('__')[0])
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
        msg = 'Failed to gather candidate tables.'
        return internal_error(msg, e)

    # If there aren't any table names, it causes an error down the code. Better
    # to return and inform them that the request wouldn't have found anything.
    if not table_names:
        return bad_request("Your request doesn't return any results. Try "
                           "adjusting your time constraint or location "
                           "parameters.")

    try:
        panel = MetaTable.timeseries_all(
            table_names, agg, start_date, end_date, geom, ctrees
        )
    except Exception as e:
        msg = 'Failed to construct timeseries.'
        return internal_error(msg, e)

    panel = MetaTable.attach_metadata(panel)
    resp = json_response_base(args, panel, args.data)

    datatype = args.data['data_type']
    if datatype == 'json':
        resp = make_response(json.dumps(resp, default=unknown_object_json_handler), 200)
        resp.headers['Content-Type'] = 'application/json'
    elif datatype == 'csv':

        # response format
        # temporal_group,dataset_name_1,dataset_name_2
        # 2014-02-24 00:00:00,235,653
        # 2014-03-03 00:00:00,156,624

        fields = ['temporal_group']
        for o in resp['objects']:
            fields.append(o['dataset_name'])

        csv_resp = []
        i = 0
        for k, g in groupby(resp['objects'], key=itemgetter('dataset_name')):
            l_g = list(g)[0]

            j = 0
            for row in l_g['items']:
                # first iteration, populate the first column with temporal_groups
                if i == 0:
                    csv_resp.append([row['datetime']])
                csv_resp[j].append(row['count'])
                j += 1
            i += 1

        csv_resp.insert(0, fields)
        csv_resp = make_csv(csv_resp)
        resp = make_response(csv_resp, 200)
        resp.headers['Content-Type'] = 'text/csv'
        filedate = datetime.now().strftime('%Y-%m-%d')
        resp.headers['Content-Disposition'] = 'attachment; filename=%s.csv' % filedate

    return resp


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

    dataset_conditions = {k: v for k, v in args.data.items() if 'filter' in k}
    for tablename, condition_tree in dataset_conditions.items():

        tablename = tablename.split('__')[0]
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
            return internal_error('Failed to construct timeseries', e)

        time_counts += [{'count': c, 'datetime': d} for c, d in ts[1:]]

    resp = None

    datatype = args.data['data_type']
    if datatype == 'json':
        resp = json_response_base(args, time_counts, request.args)
        resp['count'] = sum([c['count'] for c in time_counts])
        resp = make_response(json.dumps(resp, default=unknown_object_json_handler), 200)
        resp.headers['Content-Type'] = 'application/json'

    elif datatype == 'csv':
        resp = make_csv(time_counts)
        resp.headers['Content-Type'] = 'text/csv'
        filedate = datetime.now().strftime('%Y-%m-%d')
        resp.headers['Content-Disposition'] = 'attachment; filename=%s.csv' % filedate

    return resp


def _detail(args):

    meta_params = ('dataset', 'shape', 'data_type', 'limit', 'offset')
    meta_vals = (args.data.get(k) for k in meta_params)
    dataset, shapeset, data_type, limit, offset = meta_vals

    q = detail_query(args)

    # Apply limit and offset.
    q = q.limit(limit)
    q = q.offset(offset) if offset else q

    try:
        columns = [c.name for c in dataset.columns]
        if shapeset:
            columns += [c.name for c in shapeset.columns]
        result_rows = [OrderedDict(zip(columns, row)) for row in q.all()]
    except Exception as ex:
        session.rollback()
        return internal_error("Failed to fetch records.", ex)

    to_remove = ['point_date', 'hash']

    if data_type == 'json':
        return form_json_detail_response(to_remove, args, result_rows)

    elif data_type == 'csv':
        return form_csv_detail_response(to_remove, result_rows)

    elif data_type == 'geojson':
        return form_geojson_detail_response(to_remove, args, result_rows)


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
    filters = {k: v for k, v in args.data.items() if 'filter' in k}

    # Get upset if they specify more than a dataset and shapeset filter.
    if len(filters) > 2:
        return bad_request("Too many table filters provided.")

    # Query the point dataset.
    q = session.query(dataset)

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
            return internal_error('Could not make grid aggregation.', e)

    resp = geojson_response_base()
    for value in result_rows:
        if value[1]:
            pt = shapely.wkb.loads(value[1].decode('hex'))
            south, west = (pt.x - (size_x / 2)), (pt.y - (size_y / 2))
            north, east = (pt.x + (size_x / 2)), (pt.y + (size_y / 2))
            new_geom = shapely.geometry.box(south, west, north, east).__geo_interface__
        else:
            new_geom = None
        new_property = {'count': value[0], }
        add_geojson_feature(resp, new_geom, new_property)

    resp = make_response(json.dumps(resp, default=date_json_handler), 200)
    resp.headers['Content-Type'] = 'application/json'
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
                      'description', 'obs_from', 'obs_to', 'column_names']
    col_objects = [getattr(MetaTable, col) for col in cols_to_return]

    # Columns that need pre-processing
    col_objects.append(sqlalchemy.func.ST_AsGeoJSON(MetaTable.bbox))
    cols_to_return.append('bbox')

    # Only return datasets that have been successfully ingested
    q = session.query(*col_objects).filter(MetaTable.date_added.isnot(None))

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

    metadata_records = [dict(zip(cols_to_return, row)) for row in q.all()]
    for record in metadata_records:
        try:
            if record.get('bbox') is not None:
                # serialize bounding box geometry to string
                record['bbox'] = json.loads(record['bbox'])
            # format columns in the expected way
            record['columns'] = [{'field_name': k, 'field_type': v}
                                 for k, v in record['column_names'].items()]
        except Exception as e:
            args.warnings.append(e.message)

        # clear column_names off the json, users don't need to see it
        del record['column_names']

    resp = json_response_base(args, metadata_records, request.args)
    resp['meta']['total'] = len(resp['objects'])
    status_code = 200
    resp = make_response(json.dumps(resp, default=unknown_object_json_handler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp


# =====
# Utils
# =====

def request_args_to_condition_tree(request_args, ignore=list()):
    """Take dictionary that has a 'dataset' key and column arguments into
    a single and build a condition tree.

    :param request_args: dictionary with a dataset and column arguments
    :param ignore: what values to not use for building conditions

    :returns: condition tree"""

    ignored = {'agg', 'data_type', 'dataset', 'geom', 'limit', 'offset', 'shape', 'shapeset'}
    for val in ignore:
        ignored.add(val)

    args = request_args.copy()

    # If the key wasn't convertable, it meant that it was a column key.
    columns = {k: v for k, v in args.items() if k not in ignored}

    ctree = {"op": "and", "val": []}

    # Add AND conditions based on query string parameters.
    for k, v in columns.items():
        k = k.split('__')
        if k[0] == 'obs_date':
            k[0] = 'point_date'
        if k[0] == 'date' and 'time_of_day' in k[1]:
            k[0] = sqlalchemy.func.date_part('hour', args['dataset'].c.point_date)
            k[1] = 'le' if 'le' in k[1] else 'ge'

        if len(k) == 1:
            ctree['val'].append({"op": "eq", "col": k[0], "val": v})
        elif len(k) == 2:
            ctree['val'].append({"op": k[1], "col": k[0], "val": v})

    return ctree
