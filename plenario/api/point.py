from plenario.api.common import cache, crossdomain, CACHE_TIMEOUT, make_cache_key, \
    dthandler, make_csv, extract_first_geometry_fragment, make_fragment_str, RESPONSE_LIMIT, \
    unknownObjectHandler
from flask import request, make_response
import dateutil.parser
from datetime import timedelta, datetime
from plenario.models import MetaTable
from itertools import groupby
from operator import itemgetter
import json
from sqlalchemy import Table, text
import sqlalchemy as sa
from sqlalchemy.exc import NoSuchTableError
from plenario.database import session, Base, app_engine as engine
import shapely.wkb, shapely.geometry
from sqlalchemy.types import NullType
from collections import OrderedDict

from sqlalchemy import func, cast, select, Column, MetaData
from sqlalchemy import Integer, String
from plenario.models import ShapeMetadata

class ParamValidator(object):

    def __init__(self, dataset_name=None, shape_dataset_name=None):
        # Maps param keys to functions that validate and transform its string value.
        # Each transform returns (transformed_value, error_string)
        self.transforms = {}
        # Map param keys to usable values.
        self.vals = {}
        # Let the caller know which params we ignored.
        self.warnings = []

        if dataset_name:
            # Throws NoSuchTableError. Should be caught by caller.
            self.dataset = Table(dataset_name, Base.metadata, autoload=True,
                                 autoload_with=engine, extend_existing=True)
            self.cols = self.dataset.columns.keys()
            # SQLAlchemy boolean expressions
            self.conditions = []

            if shape_dataset_name is not None:
                self.set_shape(shape_dataset_name)

    def set_shape(self, shape_dataset_name):
        shape_table_meta = session.query(ShapeMetadata).get(shape_dataset_name)
        if shape_table_meta:
            shape_table = shape_table_meta.shape_table
            self.cols += ['{}.{}'.format(shape_table.name, key) for key in shape_table.columns.keys()]
            self.vals['shape'] = shape_table

    def set_optional(self, name, transform, default):
        """
        :param name: Name of expected HTTP parameter
        :param transform: Function of type
                          f(param_val: str) -> (validated_argument: Option<object, None>, err_msg: Option<str, None>
                          Return value should be of form (output, None) if transformation was applied successully,
                          or (None, error_message_string) if transformation could not be applied.
        :param default: Value to apply to associate with parameter given by :name
                        if not specified by user. Can be a callable.
        :return: Returns the Validator to allow generative construction.
        """
        self.vals[name] = default
        self.transforms[name] = transform

        # For call chaining
        return self

    def validate(self, params):
        for k, v in params.items():
            if k in self.transforms.keys():
                # k is a param name with a defined transformation
                # Get the transformation and apply it to v
                val, err = self.transforms[k](v)
                if err:
                    # v wasn't a valid string for this param name
                    return err
                # Override the default with the transformed value.
                self.vals[k] = val
                continue

            elif hasattr(self, 'cols'):
                # Maybe k specifies a condition on the dataset
                cond, err = self._make_condition(k, v)
                # 'if cond' fails because sqlalchemy overrides __bool__
                if cond is not None:
                    self.conditions.append(cond)
                    continue
                elif err:
                    # Valid field was specified, but operator was malformed
                    return err
                # else k wasn't an attempt at setting a condition

            # This param is neither present in the optional params
            # nor does it specify a field in this dataset.
            if k != 'shape':
                #quick and dirty way to make sure 'shape' is not listed as an unused value
                warning = 'Unused parameter value "{}={}"'.format(k, v)
                self.warnings.append(warning)

        self._eval_defaults()

    def get_geom(self):
        validated_geom = self.vals['location_geom__within']
        if validated_geom is not None:
            buff = self.vals.get('buffer', 100)
            return make_fragment_str(validated_geom, buff)

    def _eval_defaults(self):
        """
        Replace every value in vals that is callable with the returned value of that callable.
        Lets us lazily evaluate dafaults only when they aren't overridden.
        """
        for k, v in self.vals.items():
            if hasattr(v, '__call__'):
                self.vals[k] = v()

    # Map codes we accept in API docs to sqlalchemy function names
    field_ops = {
        'gt': '__gt__',
        'ge': '__ge__',
        'lt': '__lt__',
        'le': '__le__',
        'ne': '__ne__',
        'like': 'like',
        'ilike': 'ilike',
    }

    def _check_shape_condition(self, field):
        #returns false if the shape column is 
        return self.vals.get('shape') is not None\
         and '{}.{}'.format(self.vals['shape'], field) in self.cols

    def _make_condition(self, k, v):
        # Generally, we expect the form k = [field]__[op]
        # Can also be just [field] in the case of simple equality
        tokens = k.split('__')
        # An attribute of the dataset
        field = tokens[0]
        if field not in self.cols and not self._check_shape_condition(field):
            # No column matches this key.
            # Rather than return an error here,
            # we'll return None to indicate that this field wasn't present
            # and let the calling function send a warning to the client.
            
            return None, None

        col = self.dataset.columns.get(field)
        if col is None and self.vals.get('shape') is not None:
            col = self.vals['shape'].columns.get(field)

        if len(tokens) == 1:
            # One token? Then it's an equality operation of the form k=v
            # col == v creates a SQLAlchemy boolean expression
            return (col == v), None
        elif len(tokens) == 2:
            # Two tokens? Then it's of the form [field]__[op_code]=v
            op_code = tokens[1]
            valid_op_codes = self.field_ops.keys() + ['in']
            if op_code not in valid_op_codes:
                error_msg = "Invalid dataset field operator:" \
                                " {} called in {}={}".format(op_code, k, v)
                return None, error_msg
            else:
                cond = self._make_condition_with_operator(col, op_code, v)
                return cond, None

        else:
            error_msg = "Too many arguments on dataset field {}={}" \
                        "\n Expected [field]__[operator]=value".format(k, v)
            return None, error_msg

    def _make_condition_with_operator(self, col, op_code, target_value):
        if op_code == 'in':
            cond = col.in_(target_value.split(','))
            return cond
        else:   # Any other op code
            op_func = self.field_ops[op_code]
            # op_func is the name of a method bound to the SQLAlchemy column object.
            # Get the method and call it to create a binary condition (like name != 'Roy')
            # on the value the user specified.
            cond = getattr(col, op_func)(target_value)
            return cond

'''
    Validator transformations.
    To be added to a validator object with Validator.set_optional()
    They take a string and return a tuple of (expected_return_type, error_message)
    where error_message is non-None only when the transformation could not produce an object of the expected type.
'''

def setup_detail_validator(dataset_name, params):
    
    try:
        if 'shape' in params:
            shape = params['shape']
        else:
            shape = None
        validator = ParamValidator(dataset_name, shape)
    except NoSuchTableError:
        return bad_request("Cannot find dataset named {}".format(dataset_name))

    validator\
        .set_optional('obs_date__ge',
                      date_validator,
                      datetime.now() - timedelta(days=90))\
        .set_optional('obs_date__le', date_validator, datetime.now())\
        .set_optional('location_geom__within', geom_validator, None)\
        .set_optional('offset', int_validator, 0)\
        .set_optional('data_type',
                      make_format_validator(['json', 'csv', 'geojson']),
                      'json')\
        .set_optional('date__time_of_day_ge', time_of_day_validator, 0)\
        .set_optional('date__time_of_day_le', time_of_day_validator, 23)

    '''create another validator to check if shape dataset is in meta_shape, then return
    the actual table object for that shape if it is present'''

    return validator

def agg_validator(agg_str):
    VALID_AGG = ['day', 'week', 'month', 'quarter', 'year']

    if agg_str in VALID_AGG:
        return agg_str, None
    else:
        error_msg = '{} is not a valid unit of aggregation. Plenario accepts {}'\
                    .format(agg_str, ','.join(VALID_AGG))
        return None, error_msg


def date_validator(date_str):
    try:
        date = dateutil.parser.parse(date_str)
        return date, None
    except (ValueError, OverflowError):
        error_msg = 'Could not parse date string {}'.format(date_str)
        return None, error_msg


def list_of_datasets_validator(list_str):
    table_names = list_str.split(',')
    if not len(table_names) > 1:
        error_msg = "Expected comma-separated list of computer-formatted dataset names." \
                    " Couldn't parse {}".format(list_str)
        return None, error_msg
    return table_names, None


def make_format_validator(valid_formats):
    """
    :param valid_formats: A list of strings that are acceptable types of data formats.
    :return: a validator function usable by ParamValidator
    """

    def format_validator(format_str):
        if format_str in valid_formats:
            return format_str, None
        else:
            error_msg = '{} is not a valid output format. Plenario accepts {}'\
                        .format(format_str, ','.join(valid_formats))
            return error_msg, None

    return format_validator


def geom_validator(geojson_str):
    # Only extracts first geometry fragment as dict.
    try:
        fragment = extract_first_geometry_fragment(geojson_str)
        return fragment, None
    except ValueError:
        error_message = "Could not parse as geojson: {}".format(geojson_str)
        return None, error_message


def int_validator(int_str):
    try:
        num = int(int_str)
        assert(num >= 0)
        return num, None
    except (ValueError, AssertionError):
        error_message = "Could not parse as non-negative integer: {}".format(int_str)
        return None, error_message


def time_of_day_validator(hour_str):
    num, err = int_validator(hour_str)
    if err:
        return None, err
    if num > 23:
        error_message = "{} is not a valid hour of the day (Must be between 0 and 23)".format(hour_str)
        return None, error_message
    else:
        return num, None

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
                lower_bound = sa.func.date_part('hour', d.c.point_date).__ge__(start_hour)
                filters.append(lower_bound)
        except KeyError:
            pass

        try:
            end_hour = self.args['date__time_of_day_le']
            if end_hour != 23:
                upper_bound = sa.func.date_part('hour', d.c.point_date).__ge__(end_hour)
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
        return self.dataset.c.geom.ST_Within(sa.func.ST_GeomFromGeoJSON(geom_str))


def sql_ready_geom(validated_geom, buff):
    """
    :param validated_geom: geoJSON fragment as extracted from geom_validator
    :param buff: int representing lenth of buffer around geometry if geometry is a linestring
    :return: Geometry string suitable for postgres query
    """
    return make_fragment_str(validated_geom, buff)


def make_error(msg, status_code):
    resp = {
        'meta': {
            'status': 'error',
            'message': msg,
        },
        'objects': [],
    }

    resp['meta']['query'] = request.args
    return make_response(json.dumps(resp), status_code)


def bad_request(msg):
    return make_error(msg, 400)


def internal_error(context_msg, exception):
    msg = context_msg + '\nDebug:\n' + repr(exception)
    return make_error(msg, 500)

def remove_columns_from_dict(rows, col_names):
    for row in rows:
        for name in col_names:
            del row[name]

def json_response_base(validator, objects, query=''):
    meta = {
        'status': 'ok',
        'message': '',
        'query': query,
    }

    if validator:
        meta['message'] = validator.warnings
        meta['query'] = validator.vals

    return {
        'meta': meta,
        'objects': objects,
    }

def geojson_response_base():
    return {
        "type": "FeatureCollection",
        "features": []
    }

def add_geojson_feature(geojson_response, feature_geom, feature_properties):
    new_feature = {
        "type": "Feature",
        "geometry": feature_geom,
        "properties": feature_properties
    }
    geojson_response['features'].append(new_feature)

def form_json_detail_response(to_remove, validator, rows):
    to_remove.append('geom')
    remove_columns_from_dict(rows, to_remove)
    resp = json_response_base(validator, rows)
    resp['meta']['total'] = len(resp['objects'])
    resp['meta']['query'] = validator.vals
    resp = make_response(json.dumps(resp, default=unknownObjectHandler), 200)
    resp.headers['Content-Type'] = 'application/json'
    return resp

def form_csv_detail_response(to_remove, validator, rows):
    to_remove.append('geom')
    remove_columns_from_dict(rows, to_remove)

    # Column headers from arbitrary row,
    # then the values from all the others
    csv_resp = [rows[0].keys()] + [row.values() for row in rows]
    resp = make_response(make_csv(csv_resp), 200)
    dname = validator.dataset.name #dataset_name
    filedate = datetime.now().strftime('%Y-%m-%d')
    resp.headers['Content-Type'] = 'text/csv'
    resp.headers['Content-Disposition'] = 'attachment; filename=%s_%s.csv' % (dname, filedate)
    return resp

def form_geojson_detail_response(to_remove, validator, rows):
    geojson_resp = geojson_response_base()
    # We want the geom this time.
    remove_columns_from_dict(rows, to_remove)

    for row in rows:
        try:
            wkb = row.pop('geom')
            geom = shapely.wkb.loads(wkb.desc, hex=True).__geo_interface__
        except (KeyError, AttributeError):
            # If we couldn't fund a geom value,
            # or said value was not of the expected type,
            # then skip this column
            continue
        else:
            add_geojson_feature(geojson_resp, geom, row)

    resp = make_response(json.dumps(geojson_resp, default=dthandler), 200)
    resp.headers['Content-Type'] = 'application/json'
    return resp

def form_detail_sql_query(validator, aggregate_points=False):
    dset = validator.dataset
    try:
        q = session.query(dset)
        if validator.conditions:
            q = q.filter(*validator.conditions)
    except Exception as e:
        return internal_error('Failed to construct column filters.', e)

    try:
        # Add time filters
        maker = FilterMaker(validator.vals, dataset=dset)
        q = q.filter(*maker.time_filters())

        # Add geom filter, if provided
        geom = validator.get_geom()
        if geom is not None:
            geom_filter = maker.geom_filter(geom)
            q = q.filter(geom_filter)
    except Exception as e:
        return internal_error('Failed to construct time and geometry filters.', e)

    #if the query specified a shape dataset, add a join to the sql query with that dataset
    shape_table = validator.vals.get('shape')
    if shape_table != None:
        shape_columns = ['{}.{} as {}'.format(shape_table.name, col.name, col.name) for col in shape_table.c]   
        if aggregate_points: 
            q = q.from_self(shape_table).filter(dset.c.geom.ST_Intersects(shape_table.c.geom)).group_by(shape_table)
        else:
            q = q.join(shape_table, dset.c.geom.ST_Within(shape_table.c.geom))
            #add columns from shape dataset to the select statement
            q = q.add_columns(*shape_columns)

    return q

@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def timeseries():
    validator = ParamValidator()\
        .set_optional('agg', agg_validator, 'week')\
        .set_optional('data_type', make_format_validator(['json', 'csv']), 'json')\
        .set_optional('dataset_name__in', list_of_datasets_validator, MetaTable.index)\
        .set_optional('obs_date__ge', date_validator, datetime.now() - timedelta(days=90))\
        .set_optional('obs_date__le', date_validator, datetime.now())\
        .set_optional('location_geom__within', geom_validator, None)\
        .set_optional('buffer', int_validator, 100)

    err = validator.validate(request.args)
    if err:
        return bad_request(err)

    geom = validator.get_geom()
    table_names = validator.vals['dataset_name__in']
    start_date = validator.vals['obs_date__ge']
    end_date = validator.vals['obs_date__le']
    agg = validator.vals['agg']

    # Only examine tables that have a chance of containing records within the date and space boundaries.
    try:
        table_names = MetaTable.narrow_candidates(table_names, start_date, end_date, geom)
    except Exception as e:
        msg = 'Failed to gather candidate tables.'
        return internal_error(msg, e)

    try:
        panel = MetaTable.timeseries_all(table_names=table_names,
                                         agg_unit=agg,
                                         start=start_date,
                                         end=end_date,
                                         geom=geom)
    except Exception as e:
        msg = 'Failed to construct timeseries.'
        return internal_error(msg, e)

    resp = json_response_base(validator, panel)

    datatype = validator.vals['data_type']
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
        for k,g in groupby(resp['objects'], key=itemgetter('dataset_name')):
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


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def detail_aggregate():
    raw_query_params = request.args.copy()
    # First, make sure name of dataset was provided...
    try:
        dataset_name = raw_query_params.pop('dataset_name')
    except KeyError:
        return bad_request("'dataset_name' is required")

    # and that we have that dataset.
    try:
        validator = ParamValidator(dataset_name)
    except NoSuchTableError:
        return bad_request("Cannot find dataset named {}".format(dataset_name))

    validator\
        .set_optional('obs_date__ge', date_validator, datetime.now() - timedelta(days=90))\
        .set_optional('obs_date__le', date_validator, datetime.now())\
        .set_optional('location_geom__within', geom_validator, None)\
        .set_optional('data_type', make_format_validator(['json', 'csv']), 'json')\
        .set_optional('agg', agg_validator, 'week')

    # If any optional parameters are malformed, we're better off bailing and telling the user
    # than using a default and confusing them.
    err = validator.validate(raw_query_params)
    if err:
        return bad_request(err)

    start_date = validator.vals['obs_date__ge']
    end_date = validator.vals['obs_date__le']
    agg = validator.vals['agg']
    geom = validator.get_geom()
    dataset = MetaTable.get_by_dataset_name(dataset_name)

    try:
        ts = dataset.timeseries_one(agg_unit=agg, start=start_date,
                                    end=end_date, geom=geom,
                                    column_filters=validator.conditions)
    except Exception as e:
        return internal_error('Failed to construct timeseries', e)

    datatype = validator.vals['data_type']
    if datatype == 'json':
        time_counts = [{'count': c, 'datetime': d} for c, d in ts[1:]]
        resp = json_response_base(validator, time_counts)
        resp = make_response(json.dumps(resp, default=dthandler), 200)
        resp.headers['Content-Type'] = 'application/json'

    elif datatype == 'csv':
        resp = make_csv(ts)
        resp.headers['Content-Type'] = 'text/csv'
        filedate = datetime.now().strftime('%Y-%m-%d')
        resp.headers['Content-Disposition'] = 'attachment; filename=%s.csv' % filedate

    return resp

@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def detail():
    # Part 1: validate parameters
    raw_query_params = request.args.copy()
    # First, make sure name of dataset was provided...
    try:
        dataset_name = raw_query_params.pop('dataset_name')
    except KeyError:
        return bad_request("'dataset_name' is required")

    validator = setup_detail_validator(dataset_name, raw_query_params)

    # If any optional parameters are malformed, we're better off bailing and telling the user
    # than using a default and confusing them.
    err = validator.validate(raw_query_params)
    if err:
        return bad_request(err)

    #Part 2: Form SQL query from parameters stored in 'validator' object
    q = form_detail_sql_query(validator)
    
    # Page in RESPONSE_LIMIT chunks
    offset = validator.vals['offset']
    q = q.limit(RESPONSE_LIMIT)
    if offset > 0:
        q = q.offset(offset)

    # Part 3: Make SQL query and dump output into list of rows
    # (Could explicitly not request point_date and geom here
    #  to transfer less data)
    try:
        rows = [OrderedDict(zip(validator.cols, res)) for res in q.all()]
    except Exception as e:
        return internal_error('Failed to fetch records.', e)

    # Part 4: Format response
    to_remove = ['point_date', 'hash']
    if validator.vals.get('shape') is not None:
        #to_remove.append('{}.geom'.format(validator.vals['shape'].name))
        to_remove += ['{}.{}'.format(validator.vals['shape'].name, col) for col in ['geom', 'hash', 'ogc_fid']]

    datatype = validator.vals['data_type']
    if datatype == 'json':
        resp = form_json_detail_response(to_remove, validator, rows)

    elif datatype == 'csv':
        resp = form_csv_detail_response(to_remove, validator, rows)

    elif datatype == 'geojson':
        resp = form_geojson_detail_response(to_remove, validator, rows)
    
    return resp


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def grid():
    raw_query_params = request.args.copy()

    # First, make sure name of dataset was provided...
    try:
        dataset_name = raw_query_params.pop('dataset_name')
    except KeyError:
        return bad_request("'dataset_name' is required")

    try:
        validator = ParamValidator(dataset_name)
    except NoSuchTableError:
        return bad_request("Could not find dataset named {}.".format(dataset_name))

    validator.set_optional('buffer', int_validator, 100)\
             .set_optional('resolution', int_validator, 500)\
             .set_optional('location_geom__within', geom_validator, None)\
             .set_optional('obs_date__ge', date_validator, datetime.now() - timedelta(days=90))\
             .set_optional('obs_date__le', date_validator, datetime.now())\

    err = validator.validate(raw_query_params)
    if err:
        return bad_request(err)

    # Part 2: Construct SQL query
    try:
        dset = validator.dataset
        maker = FilterMaker(validator.vals, dataset=dset)
        # Get time filters
        time_filters = maker.time_filters()
        # From user params, wither get None or requested geometry
        geom = validator.get_geom()
    except Exception as e:
        return internal_error('Could not make time and geometry filters.', e)

    resolution = validator.vals['resolution']
    try:
        registry_row = MetaTable.get_by_dataset_name(dataset_name)
        grid_rows, size_x, size_y = registry_row.make_grid(resolution, geom, validator.conditions + time_filters)
    except Exception as e:
        return internal_error('Could not make grid aggregation.', e)

    resp = geojson_response_base()
    for value in grid_rows:
        if value[1]:
            pt = shapely.wkb.loads(value[1].decode('hex'))
            south, west = (pt.x - (size_x / 2)), (pt.y - (size_y /2))
            north, east = (pt.x + (size_x / 2)), (pt.y + (size_y / 2))
            new_geom = shapely.geometry.box(south, west, north, east).__geo_interface__
        else:
            new_geom = None
        new_property = {'count': value[0],}
        add_geojson_feature(resp, new_geom, new_property)

    resp = make_response(json.dumps(resp, default=dthandler), 200)
    resp.headers['Content-Type'] = 'application/json'
    return resp


@cache.cached(timeout=CACHE_TIMEOUT,
              unless=lambda: request.args.get('dataset_name') is not None)
@crossdomain(origin="*")
def meta():
    status_code = 200

    dataset_name = request.args.get('dataset_name')

    q = '''
        SELECT  m.obs_from, m.location, m.latitude, m.last_update,
                m.source_url_hash, m.attribution, m.description, m.source_url,
                m.obs_to, m.date_added, m.view_url,
                m.longitude, m.observed_date, m.human_name, m.dataset_name,
                m.update_freq, ST_AsGeoJSON(m.bbox) as bbox
        FROM meta_master AS m
        WHERE m.date_added IS NOT NULL
    '''

    if dataset_name:
        response_query = {'dataset_name': dataset_name}

        # We need to get the bbox separately so we can request it as json
        q = text('{0} AND m.dataset_name=:dataset_name'.format(q))

        with engine.begin() as c:
            metas = list(c.execute(q, dataset_name=dataset_name))
    else:
        response_query = None
        with engine.begin() as c:
            metas = list(c.execute(q))

    resp = json_response_base(None, [], query=response_query)

    for m in metas:
        keys = dict(zip(m.keys(), m.values()))
        # If we have bounding box data, add it
        if (m['bbox'] is not None):
            keys['bbox'] = json.loads(m.bbox)
        resp['objects'].append(keys)

    resp['meta']['total'] = len(resp['objects'])
    resp = make_response(json.dumps(resp, default=dthandler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp


@cache.cached(timeout=CACHE_TIMEOUT)
@crossdomain(origin="*")
def dataset_fields(dataset_name):
    try:
        table = Table(dataset_name, Base.metadata,
                      autoload=True, autoload_with=engine,
                      extend_existing=True)

        resp = json_response_base(None, [], query={'dataset_name': dataset_name})
        status_code = 200
        for col in table.columns:
            if not isinstance(col.type, NullType):
                # Don't report our bookkeeping columns
                if col.name in {'geom', 'point_date'}:
                    continue
                    
                d = {}
                d['field_name'] = col.name
                d['field_type'] = str(col.type)
                resp['objects'].append(d)
        resp = make_response(json.dumps(resp), status_code)

    except NoSuchTableError:
        error_msg = "'%s' is not a valid table name" % dataset_name
        resp = bad_request(error_msg)

    resp.headers['Content-Type'] = 'application/json'
    return resp
