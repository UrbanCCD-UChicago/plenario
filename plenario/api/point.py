import json
import shapely.geometry
import shapely.wkb
import sqlalchemy
import traceback

from collections import OrderedDict
from datetime import timedelta, datetime
from flask import request, make_response
from itertools import groupby
from operator import itemgetter
from sqlalchemy import Table
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.types import NullType

from plenario.api.common import cache, crossdomain, CACHE_TIMEOUT, make_cache_key, dthandler, make_csv
from plenario.api.common import make_fragment_str, RESPONSE_LIMIT
from plenario.api.errors import bad_request, internal_error
from plenario.api.filters import FilterMaker
from plenario.api.responses import json_response_base, form_csv_detail_response, form_detail_sql_query
from plenario.api.responses import form_json_detail_response, geojson_response_base, form_geojson_detail_response
from plenario.api.responses import add_geojson_feature
from plenario.api.validators import ParamValidator
from plenario.api.validators import setup_detail_validator, agg_validator, date_validator, list_of_datasets_validator
from plenario.api.validators import make_format_validator, geom_validator, int_validator, no_op_validator
from plenario.database import session, Base, app_engine as engine
from plenario.models import MetaTable


def sql_ready_geom(validated_geom, buff):
    """
    :param validated_geom: geoJSON fragment as extracted from geom_validator
    :param buff: int representing lenth of buffer around geometry if geometry is a linestring
    :return: Geometry string suitable for postgres query
    """
    return make_fragment_str(validated_geom, buff)


def make_field_query(dataset_name):
    table = Table(dataset_name, Base.metadata, autoload=True, autoload_with=engine, extend_existing=True)

    cols = []
    for col in table.columns:
        if not isinstance(col.type, NullType):
            # Don't report our bookkeeping columns
            if col.name in {'geom', 'point_date', 'hash'}:
                continue

            d = {
                'field_name': col.name,
                'field_type': str(col.type)
            }
            cols.append(d)

    return cols


# ===========
# API Methods
# ===========

@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def timeseries():
    validator = ParamValidator() \
        .set_optional('agg', agg_validator, 'week') \
        .set_optional('data_type', make_format_validator(['json', 'csv']), 'json') \
        .set_optional('dataset_name__in', list_of_datasets_validator, MetaTable.index) \
        .set_optional('obs_date__ge', date_validator, datetime.now() - timedelta(days=90)) \
        .set_optional('obs_date__le', date_validator, datetime.now()) \
        .set_optional('location_geom__within', geom_validator, None) \
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

    panel = MetaTable.attach_metadata(panel)
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

    validator \
        .set_optional('obs_date__ge', date_validator, datetime.now() - timedelta(days=90)) \
        .set_optional('obs_date__le', date_validator, datetime.now()) \
        .set_optional('location_geom__within', geom_validator, None) \
        .set_optional('data_type', make_format_validator(['json', 'csv']), 'json') \
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

    resp = None

    datatype = validator.vals['data_type']
    if datatype == 'json':
        time_counts = [{'count': c, 'datetime': d} for c, d in ts[1:]]
        resp = json_response_base(validator, time_counts)
        resp['count'] = sum([c['count'] for c in time_counts])
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

    # If any optional parameters are malformed,
    # we're better off bailing and telling the user
    # than using a default and confusing them.
    err = None
    try:
        err = validator.validate(raw_query_params)
    except Exception, ex:
        traceback.print_exc()

    if err:
        return bad_request(err)

    # Part 2: Form SQL query from parameters stored in 'validator' object
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
    resp = None

    to_remove = ['point_date', 'hash']
    if validator.vals.get('shape') is not None:
        # to_remove.append('{}.geom'.format(validator.vals['shape'].name))
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

    validator.set_optional('buffer', int_validator, 100) \
        .set_optional('resolution', int_validator, 500) \
        .set_optional('location_geom__within', geom_validator, None) \
        .set_optional('obs_date__ge', date_validator, datetime.now() - timedelta(days=90)) \
        .set_optional('obs_date__le', date_validator, datetime.now())

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


# Kludge until we store column data in registry.
# Only cache response if it is requesting column metadata.
@cache.cached(timeout=CACHE_TIMEOUT,
              unless=lambda: request.args.get('include_columns') is None)
@crossdomain(origin="*")
def meta():
    # Doesn't require a table lookup,
    # so no params passed on construction
    validator = ParamValidator()
    validator.set_optional('dataset_name',
                           no_op_validator,
                           None) \
        .set_optional('location_geom__within',
                      geom_validator,
                      None) \
        .set_optional('obs_date__ge', date_validator, None) \
        .set_optional('obs_date__le', date_validator, None)

    err = validator.validate(request.args)
    if err:
        return bad_request(err)

    # Columns to select as-is
    cols_to_return = ['human_name', 'dataset_name',
                      'source_url', 'view_url',
                      'date_added', 'last_update', 'update_freq',
                      'attribution', 'description',
                      'obs_from', 'obs_to']
    col_objects = [getattr(MetaTable, col) for col in cols_to_return]

    # Columns that need pre-processing
    col_objects.append(sqlalchemy.func.ST_AsGeoJSON(MetaTable.bbox))
    cols_to_return.append('bbox')

    # Only return datasets that have been successfully ingested
    q = session.query(*col_objects).filter(MetaTable.date_added is not None)

    # What params did the user provide?
    dataset_name = validator.vals['dataset_name']
    geom = validator.get_geom()
    start_date = validator.vals['obs_date__ge']
    end_date = validator.vals['obs_date__le']

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
            q = q.filter(sqlalchemy.and_(
                MetaTable.obs_from < end_date,
                MetaTable.obs_to > start_date
            ))
    # Otherwise, just send back all the datasets

    metadata_records = [dict(zip(cols_to_return, row)) for row in q.all()]
    # Serialize bounding box geometry to string
    for record in metadata_records:
        if record.get('bbox') is not None:
            record['bbox'] = json.loads(record['bbox'])

    # TODO: Store this data statically in the registry, and remove conditional.
    failure_messages = []
    if request.args.get('include_columns'):
        for record in metadata_records:
            try:
                cols = make_field_query(record['dataset_name'])
                record['columns'] = cols
            except Exception as e:
                record['columns'] = None
                failure_messages.append(e.message)

    resp = json_response_base(validator, metadata_records)

    resp['meta']['total'] = len(resp['objects'])
    resp['meta']['message'] = failure_messages
    status_code = 200
    resp = make_response(json.dumps(resp, default=dthandler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp


@cache.cached(timeout=CACHE_TIMEOUT)
@crossdomain(origin="*")
def dataset_fields(dataset_name):
    try:
        resp = json_response_base(None, [],
                                  query={'dataset_name': dataset_name})
        status_code = 200
        resp['objects'] = make_field_query(dataset_name)
        resp = make_response(json.dumps(resp), status_code)

    except NoSuchTableError:
        error_msg = "'%s' is not a valid table name" % dataset_name
        resp = bad_request(error_msg)

    resp.headers['Content-Type'] = 'application/json'
    return resp
