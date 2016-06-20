import dateutil.parser
import json
import shapely.geometry
import shapely.wkb
import sqlalchemy

from collections import OrderedDict
from datetime import timedelta, datetime
from flask import request, make_response
from itertools import groupby
from operator import itemgetter
from sqlalchemy.exc import NoSuchTableError

from plenario.api.common import cache, crossdomain, CACHE_TIMEOUT, make_csv
from plenario.api.common import extract_first_geometry_fragment
from plenario.api.common import make_cache_key, dthandler, make_fragment_str
from plenario.api.common import RESPONSE_LIMIT, unknownObjectHandler
from plenario.api.condition_builder import field_ops
from plenario.api.response import *  # TODO: Correct your laziness.
from plenario.api.validator import Validator, DatasetRequiredValidator, validate
from plenario.database import session, Base, app_engine as engine
from plenario.models import MetaTable, ShapeMetadata


def _make_condition(table, k, v):
    # Generally, we expect the form k = [field]__[op]
    # Can also be just [field] in the case of simple equality
    tokens = k.split('__')
    # An attribute of the dataset
    field = tokens[0]

    col = table.columns.get(field)

    if len(tokens) == 1:
        # One token? Then it's an equality operation of the form k=v
        # col == v creates a SQLAlchemy boolean expression
        return (col == v), None
    elif len(tokens) == 2:
        # Two tokens? Then it's of the form [field]__[op_code]=v
        op_code = tokens[1]
        valid_op_codes = field_ops.keys() + ['in']
        if op_code not in valid_op_codes:
            error_msg = "Invalid dataset field operator:" \
                        " {} called in {}={}".format(op_code, k, v)
            return None, error_msg
        else:
            cond = _make_condition_with_operator(col, op_code, v)
            return cond, None

    else:
        error_msg = "Too many arguments on dataset field {}={}" \
                    "\n Expected [field]__[operator]=value".format(k, v)
        return None, error_msg


def _make_condition_with_operator(col, op_code, target_value):
    if op_code == 'in':
        cond = col.in_(target_value.split(','))
        return cond
    else:  # Any other op code
        op_func = field_ops[op_code]
        # op_func is the name of a method bound to the SQLAlchemy column object.
        # Get the method and call it to create a binary condition (like name != 'Roy')
        # on the value the user specified.
        cond = getattr(col, op_func)(target_value)
        return cond


def setup_detail_validator(dataset_name, params):

    return DatasetRequiredValidator()


class FilterMaker(object):
    """
    Given dictionary of validated arguments and a sqlalchemy table,
    generate binary consitions on that table restricting time and geography.
    Can also create a postgres-formatted geography for further filtering
    with just a dict of arguments.
    """

    def __init__(self, args, dataset=None):
        """
        :param args: dict mapping arguments to values as taken from a Validator
        :param dataset: table object of particular dataset being queried, if available
        """
        self.args = args
        self.dataset = dataset

    def time_filters(self):
        """
        :return: SQLAlchemy conditions derived from time arguments on :dataset:
        """
        filters = []
        d = self.dataset
        try:
            lower_bound = d.c.point_date >= self.args['obs_date__ge']
            filters.append(lower_bound)
        except KeyError:
            pass

        try:
            upper_bound = d.c.point_date <= self.args['obs_date__le']
            filters.append(upper_bound)
        except KeyError:
            pass

        try:
            start_hour = self.args['date__time_of_day_ge']
            if start_hour != 0:
                lower_bound = sqlalchemy.func.date_part('hour', d.c.point_date).__ge__(start_hour)
                filters.append(lower_bound)
        except KeyError:
            pass

        try:
            end_hour = self.args['date__time_of_day_le']
            if end_hour != 23:
                upper_bound = sqlalchemy.func.date_part('hour', d.c.point_date).__ge__(end_hour)
                filters.append(upper_bound)
        except KeyError:
            pass

        return filters

    def geom_filter(self, geom_str):
        """
        :param geom_str: geoJSON string from Validator ready to throw into postgres
        :return: geographic filter based on location_geom__within and buffer parameters
        """
        # Demeter weeps
        return self.dataset.c.geom.ST_Within(sqlalchemy.func.ST_GeomFromGeoJSON(geom_str))

    def column_filter(self, column_str):
        column, value = column_str.split("=")
        return _make_condition(self.dataset, column, value)


def sql_ready_geom(validated_geom, buff):
    """
    :param validated_geom: geoJSON fragment as extracted from geom_validator
    :param buff: int representing lenth of buffer around geometry if geometry is a linestring
    :return: Geometry string suitable for postgres query
    """
    return make_fragment_str(validated_geom, buff)


def form_detail_sql_query(validator, aggregate_points=False):
    dset = validator.data['dataset']
    try:
        q = session.query(dset.name)
        if validator.filters:
            q = q.filter(*validator.filters)
    except Exception as e:
        return internal_error('Failed to construct column filters.', e)

    try:
        # Add time filters
        maker = FilterMaker(validator.data, dataset=dset)
        q = q.filter(*maker.time_filters())

        # Add geom filter, if provided
        geom = validator.data['geom']
        if geom is not None:
            geom_filter = maker.geom_filter(geom)
            q = q.filter(geom_filter)
    except Exception as e:
        return internal_error('Failed to construct time and geometry filters.', e)

    # if the query specified a shape dataset, add a join to the sql query with that dataset
    shape_table = validator.data.get('shape')
    if shape_table is not None:
        shape_columns = ['{}.{} as {}'.format(shape_table.name, col.name, col.name) for col in shape_table.c]
        if aggregate_points:
            q = q.from_self(shape_table).filter(dset.c.geom.ST_Intersects(shape_table.c.geom)).group_by(shape_table)
        else:
            q = q.join(shape_table, dset.c.geom.ST_Within(shape_table.c.geom))
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
    request_args['dataset'] = dataset_name
    validated_args = validate(DatasetRequiredValidator, request_args)
    if validated_args.errors:
        return bad_request(validated_args.errors)
    return _meta(validated_args)


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def meta():
    validated_args = validate(Validator, request.args.to_dict())
    if validated_args.errors:
        return bad_request(validated_args.errors)
    return _meta(validated_args)


# ============
# _route logic
# ============

def _timeseries(args):

    geom = None
    if args.data['geom']:
        geom = make_fragment_str(args.data['geom'], args.data['buffer'])
    table_names = args.data['dataset_name__in']
    start_date = args.data['obs_date__ge']
    end_date = args.data['obs_date__le']
    agg = args.data['agg']

    # Only examine tables that have a chance of containing records within the date and space boundaries.
    try:
        table_names = MetaTable.narrow_candidates(table_names, start_date, end_date, geom)
    except Exception as e:
        msg = 'Failed to gather candidate tables.'
        return internal_error(msg, e)

    try:
        panel = MetaTable.timeseries_all(
            table_names, agg, start_date, end_date, geom
        )
    except Exception as e:
        msg = 'Failed to construct timeseries.'
        return internal_error(msg, e)

    panel = MetaTable.attach_metadata(panel)
    resp = json_response_base(args, panel, request.args)

    datatype = args.data['data_type']
    if datatype == 'json':
        resp = make_response(json.dumps(resp, default=dthandler), 200)
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

    start_date = args.data['obs_date__ge']
    end_date = args.data['obs_date__le']
    agg = args.data['agg']
    geom = args.data['geom']
    dataset = args.data['dataset']

    conditions = []
    for str_pair in args.filters:
        key, value = str_pair.split('=')
        condition, error = _make_condition(dataset.point_table, key, value)
        if condition is not None:
            conditions.append(condition)
        else:
            raise bad_request(error)

    try:
        ts = dataset.timeseries_one(
            agg, start_date, end_date, geom, conditions  # TODO: We need conditions!
        )
    except Exception as e:
        return internal_error('Failed to construct timeseries', e)

    resp = None

    datatype = args.data['data_type']
    if datatype == 'json':
        time_counts = [{'count': c, 'datetime': d} for c, d in ts[1:]]
        resp = json_response_base(args, time_counts)
        resp['count'] = sum([c['count'] for c in time_counts])
        resp = make_response(json.dumps(resp, default=dthandler), 200)
        resp.headers['Content-Type'] = 'application/json'

    elif datatype == 'csv':
        resp = make_csv(ts)
        resp.headers['Content-Type'] = 'text/csv'
        filedate = datetime.now().strftime('%Y-%m-%d')
        resp.headers['Content-Disposition'] = 'attachment; filename=%s.csv' % filedate

    return resp


def _detail(args):

    # Need this for detail.
    args.data['dataset'] = args.data['dataset'].point_table

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
        rows = [OrderedDict(zip(args.data['dataset'].columns, res)) for res in q.all()]
    except Exception as e:
        return internal_error('Failed to fetch records.', e)

    # Part 4: Format response
    to_remove = ['point_date', 'hash']
    if args.data.get('shape') is not None:
        to_remove += ['{}.{}'.format(args.data['shape'].name, col) for col in ['geom', 'hash', 'ogc_fid']]

    datatype = args.data['data_type']

    if datatype == 'json':
        return form_json_detail_response(to_remove, args, rows)

    elif datatype == 'csv':
        return form_csv_detail_response(to_remove, args, rows)

    elif datatype == 'geojson':
        return form_geojson_detail_response(to_remove, args, rows)


def _grid(args):

    # construct SQL query
    try:
        maker = FilterMaker(args.data, args.data['dataset'])
        # Get time filters
        time_filters = maker.time_filters()
        # From user params, wither get None or requested geometry
        geom = args.data['geom']
    except Exception as e:
        return internal_error('Could not make time and geometry filters.', e)

    resolution = args.data['resolution']
    try:
        registry_row = MetaTable.get_by_dataset_name(args.data['dataset'])
        grid_rows, size_x, size_y = registry_row.make_grid(resolution, geom, args.conditions + time_filters)
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

    resp = make_response(json.dumps(resp, default=dthandler), 200)
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
    dataset_name = args.vals['dataset_name']
    geom = args.get_geom()
    start_date = args.vals['obs_date__ge']
    end_date = args.vals['obs_date__le']

    # Filter over datasets if user provides full date range or geom
    should_filter = geom or (start_date and end_date)

    if dataset_name:
        # If the user specified a name, don't try any filtering.
        # Just spit back that dataset's metadata.
        q = q.filter(MetaTable.dataset_name == dataset_name)
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

    # Otherwise, just send back all the datasets
    failure_messages = []

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

    resp = json_response_base(args, metadata_records)

    resp['meta']['total'] = len(resp['objects'])
    resp['meta']['message'] = failure_messages
    status_code = 200
    resp = make_response(json.dumps(resp, default=dthandler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp
