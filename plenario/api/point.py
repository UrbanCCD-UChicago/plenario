import json
import shapely.geometry
import shapely.wkb
import sqlalchemy

from collections import OrderedDict
from datetime import datetime
from flask import request, make_response
from itertools import groupby
from operator import itemgetter

from plenario.api.common import cache, crossdomain, CACHE_TIMEOUT, RESPONSE_LIMIT
from plenario.api.common import make_cache_key, date_json_handler, unknown_object_json_handler
from plenario.api.condition_builder import parse_general, general_filters
from plenario.api.response import internal_error, bad_request, json_response_base, make_csv
from plenario.api.response import geojson_response_base, form_csv_detail_response, form_json_detail_response
from plenario.api.response import form_geojson_detail_response, add_geojson_feature
from plenario.api.validator import Validator, DatasetRequiredValidator
from plenario.api.validator import NoDefaultDatesValidator, validate
from plenario.database import session
from plenario.models import MetaTable


def form_detail_sql_query(args, aggregate_points=False):

    point_table = args.data.get('dataset')
    shape_table = args.data.get('shape')
    point_columns = point_table.columns.keys()
    shape_columns = shape_table.columns.keys() if shape_table is not None else None

    conditions = []
    for field, value in args.data.items():
        if value is not None:
            condition = None

            if shape_columns and field.split('__')[0] in shape_columns:
                condition = parse_general(shape_table, field, value)
            else:
                condition = parse_general(point_table, field, value)

            if condition is not None:
                conditions.append(condition)

    try:
        q = session.query(point_table)
        if conditions:
            q = q.filter(*conditions)
    except Exception as e:
        return internal_error('Failed to construct filters.', e)

    # if the query specified a shape dataset, add a join to the sql query with that dataset
    if shape_table is not None:
        shape_columns = ['{}.{} as {}'.format(shape_table.name, col.name, col.name) for col in shape_table.c]
        if aggregate_points:
            q = q.from_self(shape_table)\
                .filter(point_table.c.geom.ST_Intersects(shape_table.c.geom))\
                .group_by(shape_table)
        else:
            q = q.join(shape_table, point_table.c.geom.ST_Within(shape_table.c.geom))
            # add columns from shape dataset to the select statement
            q = q.add_columns(*shape_columns)

    return q


# ======
# routes
# ======

@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def timeseries():
    validated_args = validate(Validator, request.args.to_dict())
    if validated_args.errors:
        return bad_request(validated_args)
    return _timeseries(validated_args)


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def detail_aggregate():
    validated_args = validate(DatasetRequiredValidator, request.args.to_dict())
    if validated_args.errors:
        return bad_request(validated_args)
    return _detail_aggregate(validated_args)


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def detail():
    validated_args = validate(DatasetRequiredValidator, request.args.to_dict())
    if validated_args.errors:
        return bad_request(validated_args)
    return _detail(validated_args)


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def grid():
    validated_args = validate(DatasetRequiredValidator, request.args.to_dict())
    if validated_args.errors:
        return bad_request(validated_args)
    return _grid(validated_args)


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def dataset_fields(dataset_name):
    request_args = request.args.to_dict()
    request_args['dataset_name'] = dataset_name
    validated_args = validate(DatasetRequiredValidator, request_args)
    if validated_args.errors:
        return bad_request(validated_args.errors)
    return _meta(validated_args)


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def meta():
    validated_args = validate(NoDefaultDatesValidator, request.args.to_dict())
    if validated_args.errors:
        return bad_request(validated_args.errors)
    return _meta(validated_args)


# ============
# _route logic
# ============

def _timeseries(args):

    geom = args.data['geom']
    dataset = args.data.get('dataset')
    table_names = args.data['dataset_name__in']
    start_date = args.data['obs_date__ge']
    end_date = args.data['obs_date__le']
    agg = args.data['agg']

    # if a single dataset was provided, it's the only thing we need to consider
    if dataset is not None:
        table_names = [dataset.name]
        # for the query's meta information, so that it doesn't show the index
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
            table_names, agg, start_date, end_date, geom
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
        resp.headers['Content-Disposit""ion'] = 'attachment; filename=%s.csv' % filedate

    return resp


def _detail_aggregate(args):

    start_date = args.data['obs_date__ge']
    end_date = args.data['obs_date__le']
    agg = args.data['agg']
    geom = args.data['geom']
    metatable = args.data['metatable']
    dataset = args.data['dataset']

    conditions = []
    for key, value in args.data.items():
        # These conditions are only meant to address columns, so let's ignore
        # date and geom filters.
        if key in general_filters:
            pass
        else:
            condition = parse_general(dataset, key, value)
            if condition is not None:
                conditions.append(condition)

    try:
        ts = metatable.timeseries_one(
            agg, start_date, end_date, geom, conditions
        )
    except Exception as e:
        return internal_error('Failed to construct timeseries', e)

    resp = None

    datatype = args.data['data_type']
    if datatype == 'json':
        time_counts = [{'count': c, 'datetime': d} for c, d in ts[1:]]
        resp = json_response_base(args, time_counts, request.args)
        resp['count'] = sum([c['count'] for c in time_counts])
        resp = make_response(json.dumps(resp, default=unknown_object_json_handler), 200)
        resp.headers['Content-Type'] = 'application/json'

    elif datatype == 'csv':
        resp = make_csv(ts)
        resp.headers['Content-Type'] = 'text/csv'
        filedate = datetime.now().strftime('%Y-%m-%d')
        resp.headers['Content-Disposition'] = 'attachment; filename=%s.csv' % filedate

    return resp


def _detail(args):

    # Part 2: Form SQL query from parameters stored in 'validator' object
    q = form_detail_sql_query(args)

    # Page in RESPONSE_LIMIT chunks
    offset = args.data['offset']
    q = q.limit(RESPONSE_LIMIT)
    if offset > 0:
        q = q.offset(offset)

    # Part 3: Make SQL query and dump output into list of rows
    # (Could explicitly not request point_date and geom here
    #  to transfer less data)
    try:
        columns = [col.name for col in args.data['dataset'].columns]
        if args.data['shape'] is not None:
            columns += [str(col) for col in args.data['shape'].columns]
        rows = [OrderedDict(zip(columns, res)) for res in q.all()]
    except Exception as e:
        return internal_error('Failed to fetch records.', e)

    # Part 4: Format response
    to_remove = ['point_date', 'hash']
    if args.data.get('shape') is not None:
        to_remove += ['{}.{}'.format(args.data['shape'].name, col) for col in ['geom', 'hash', 'ogc_fid']]

    datatype = args.data['data_type']
    del args.data['dataset_name__in']

    if datatype == 'json':
        return form_json_detail_response(to_remove, args, rows)

    elif datatype == 'csv':
        return form_csv_detail_response(to_remove, rows)

    elif datatype == 'geojson':
        return form_geojson_detail_response(to_remove, args, rows)


def _grid(args):

    meta_table = args.data['metatable']
    point_table = args.data['dataset']
    geom = args.data['geom']
    resolution = args.data['resolution']

    # construct SQL filters
    conditions = []
    for field, value in args.data.items():
        if value is not None:
            condition = parse_general(point_table, field, value)
            if condition is not None:
                conditions.append(condition)

    try:
        registry_row = meta_table
        grid_rows, size_x, size_y = registry_row.make_grid(resolution, geom, conditions)
    except Exception as e:
        return internal_error('Could not make grid aggregation.', e)

    resp = geojson_response_base()
    for value in grid_rows:
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

    # What params did the user provide?
    dataset = args.data['dataset']
    geom = args.data['geom']
    start_date = args.data['obs_date__ge']
    end_date = args.data['obs_date__le']

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

    failure_messages = []
    failure_messages.append(args.warnings)

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
            failure_messages.append(e.message)

        # clear column_names off the json, users don't need to see it
        del record['column_names']

    resp = json_response_base(args, metadata_records, request.args)

    resp['meta']['total'] = len(resp['objects'])
    resp['meta']['message'] = failure_messages
    status_code = 200
    resp = make_response(json.dumps(resp, default=unknown_object_json_handler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp
