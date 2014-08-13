from flask import make_response, request, render_template, current_app, g, \
    Blueprint, abort
from functools import update_wrapper
import os
import math
from datetime import date, datetime, timedelta
import time
import json
from sqlalchemy import func, distinct, Column, Float, Table
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.types import NullType
from sqlalchemy.sql.expression import cast
from geoalchemy2 import Geometry
from operator import itemgetter
from itertools import groupby
from cStringIO import StringIO
import csv
from shapely.wkb import loads
from shapely.geometry import box, asShape
from collections import OrderedDict
from urlparse import urlparse

from plenario.models import MasterTable, MetaTable
from plenario.database import session, app_engine as engine, Base
from plenario.utils.helpers import get_socrata_data_info, slugify
from plenario.tasks import add_dataset

api = Blueprint('api', __name__)

dthandler = lambda obj: obj.isoformat() if isinstance(obj, date) else None

def crossdomain(origin=None, methods=None, headers=None,
                max_age=21600, attach_to_all=True,
                automatic_options=True): # pragma: no cover
    if methods is not None:
        methods = ', '.join(sorted(x.upper() for x in methods))
    if headers is not None and not isinstance(headers, basestring):
        headers = ', '.join(x.upper() for x in headers)
    if not isinstance(origin, basestring):
        origin = ', '.join(origin)
    if isinstance(max_age, timedelta):
        max_age = max_age.total_seconds()

    def get_methods():
        if methods is not None:
            return methods

        options_resp = current_app.make_default_options_response()
        return options_resp.headers['allow']

    def decorator(f):
        def wrapped_function(*args, **kwargs):
            if automatic_options and request.method == 'OPTIONS':
                resp = current_app.make_default_options_response()
            else:
                resp = make_response(f(*args, **kwargs))
            if not attach_to_all and request.method != 'OPTIONS':
                return resp

            h = resp.headers

            h['Access-Control-Allow-Origin'] = origin
            h['Access-Control-Allow-Methods'] = get_methods()
            h['Access-Control-Max-Age'] = str(max_age)
            if headers is not None:
                h['Access-Control-Allow-Headers'] = headers
            return resp

        f.provide_automatic_options = False
        return update_wrapper(wrapped_function, f)
    return decorator

def make_query(table, raw_query_params):
    table_keys = table.columns.keys()
    args_keys = raw_query_params.keys()
    resp = {
        'meta': {
            'status': 'error',
            'message': '',
        },
        'objects': [],
    }
    status_code = 200
    query_clauses = []
    valid_query = True
    if 'offset' in args_keys:
        args_keys.remove('offset')
    if 'limit' in args_keys:
        args_keys.remove('limit')
    if 'order_by' in args_keys:
        args_keys.remove('order_by')
    for query_param in args_keys:
        try:
            field, operator = query_param.split('__')
        except ValueError:
            field = query_param
            operator = 'eq'
        query_value = raw_query_params.get(query_param)
        column = table.columns.get(field)
        if field not in table_keys:
            resp['meta']['message'] = '"%s" is not a valid fieldname' % field
            status_code = 400
            valid_query = False
        elif operator == 'in':
            query = column.in_(query_value.split(','))
            query_clauses.append(query)
        elif operator == 'within':
            geo = json.loads(query_value)
            if 'features' in geo.keys():
                val = geo['features'][0]['geometry']
            elif 'geometry' in geo.keys():
                val = geo['geometry']
            else:
                val = geo
            if val['type'] == 'LineString':
                shape = asShape(val)
                lat = shape.centroid.y
                # 100 meters by default
                x, y = getSizeInDegrees(100, lat)
                val = shape.buffer(y).__geo_interface__
            val['crs'] = {"type":"name","properties":{"name":"EPSG:4326"}}
            query = column.ST_Within(func.ST_GeomFromGeoJSON(json.dumps(val)))
            query_clauses.append(query)
        elif operator.startswith('time_of_day'):
            if operator.endswith('ge'):
                query = func.date_part('hour', column).__ge__(query_value)
            elif operator.endswith('le'):
                query = func.date_part('hour', column).__le__(query_value)
            query_clauses.append(query)
        else:
            try:
                attr = filter(
                    lambda e: hasattr(column, e % operator),
                    ['%s', '%s_', '__%s__']
                )[0] % operator
            except IndexError:
                resp['meta']['message'] = '"%s" is not a valid query operator' % operator
                status_code = 400
                valid_query = False
                break
            if query_value == 'null': # pragma: no cover
                query_value = None
            query = getattr(column, attr)(query_value)
            query_clauses.append(query)
    return valid_query, query_clauses, resp, status_code

@api.route('/api/')
@crossdomain(origin="*")
def meta():
    status_code = 200
    resp = []
    dataset_name = request.args.get('dataset_name')
    if dataset_name:
        metas = session.query(MetaTable)\
            .filter(MetaTable.dataset_name == dataset_name).all()
    else:
        metas = session.query(MetaTable).all()
    resp.extend([m.as_dict() for m in metas])
    resp = make_response(json.dumps(resp, default=dthandler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp

@api.route('/api/fields/<dataset_name>/')
@crossdomain(origin="*")
def dataset_fields(dataset_name):
    try:
        table = Table('dat_%s' % dataset_name, Base.metadata,
            autoload=True, autoload_with=engine,
            extend_existing=True)
        data = {
            'meta': {
                'status': 'ok',
                'message': ''
            },
            'objects': []
        }
        status_code = 200
        table_exists = True
    except NoSuchTableError:
        table_exists = False
        data = {
            'meta': {
                'status': 'error',
                'message': "'%s' is not a valid table name" % dataset_name
            },
            'objects': []
        }
        status_code = 400
    if table_exists:
        fields = table.columns.keys()
        for col in table.columns:
            if not isinstance(col.type, NullType):
                d = {}
                d['field_name'] = col.name
                d['field_type'] = str(col.type)
                data['objects'].append(d)
    resp = make_response(json.dumps(data), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp

def make_csv(data):
    outp = StringIO()
    writer = csv.writer(outp)
    writer.writerows(data)
    return outp.getvalue()

@api.route('/api/master/')
@crossdomain(origin="*")
def dataset():
    raw_query_params = request.args.copy()
    agg = raw_query_params.get('agg')
    if not agg:
        agg = 'day'
    else:
        del raw_query_params['agg']
    datatype = 'json'
    if raw_query_params.get('datatype'):
        datatype = raw_query_params['datatype']
        del raw_query_params['datatype']
    mt = MasterTable.__table__
    valid_query, query_clauses, resp, status_code = make_query(mt,raw_query_params)
    if valid_query:
        time_agg = func.date_trunc(agg, mt.c['obs_date'])
        base_query = session.query(time_agg, 
            func.count(mt.c['obs_date']),
            mt.c['dataset_name'])
        base_query = base_query.filter(mt.c['current_flag'] == True)
        for clause in query_clauses:
            base_query = base_query.filter(clause)
        base_query = base_query.group_by(mt.c['dataset_name'])\
            .group_by(time_agg)\
            .order_by(time_agg)
        values = [o for o in base_query.all()]
        results = []
        for value in values:
            d = {
                'dataset_name': value[2],
                'group': value[0],
                'count': value[1],
                }
            results.append(d)
        results = sorted(results, key=itemgetter('dataset_name'))
        for k,g in groupby(results, key=itemgetter('dataset_name')):
            d = {'dataset_name': k}
            d['temporal_aggregate'] = agg
            d['items'] = list(g)
            resp['objects'].append(d)
        resp['meta']['status'] = 'ok'
    if datatype == 'json':
        resp = make_response(json.dumps(resp, default=dthandler), status_code)
        resp.headers['Content-Type'] = 'application/json'
    elif datatype == 'csv':
        csv_resp = []
        fields = ['temporal_group']
        results = sorted(results, key=itemgetter('group'))
        for k,g in groupby(results, key=itemgetter('group')):
            d = [k]
            for row in list(g):
                if row['dataset_name'] not in fields:
                    fields.append(row['dataset_name'])
                d.append(row['count'])
            csv_resp.append(d)
        csv_resp[0] = fields
        csv_resp = make_csv(csv_resp)
        resp = make_response(csv_resp, 200)
        resp.headers['Content-Type'] = 'text/csv'
        filedate = datetime.now().strftime('%Y-%m-%d')
        resp.headers['Content-Disposition'] = 'attachment; filename=%s.csv' % (filedate)
    return resp

def parse_join_query(params):
    queries = {
        'base' : {},
        'detail': {},
    }
    agg = 'day'
    datatype = 'json'
    for key, value in params.items():
        if key.split('__')[0] in ['obs_date', 'location_geom', 'dataset_name']:
            queries['base'][key] = value
        elif key == 'agg':
            agg = value
        elif key == 'datatype':
            datatype = value
        else:
            queries['detail'][key] = value
    return agg, datatype, queries

@api.route('/api/detail/')
@crossdomain(origin="*")
def detail():
    raw_query_params = request.args.copy()
    agg, datatype, queries = parse_join_query(raw_query_params)
    limit = raw_query_params.get('limit')
    order_by = raw_query_params.get('order_by')
    mt = MasterTable.__table__
    valid_query, base_clauses, resp, status_code = make_query(mt, queries['base'])
    if valid_query:
        resp['meta']['status'] = 'ok'
        dname = raw_query_params['dataset_name']
        dataset = Table('dat_%s' % dname, Base.metadata,
            autoload=True, autoload_with=engine,
            extend_existing=True)
        base_query = session.query(mt.c.obs_date, dataset)
        valid_query, detail_clauses, resp, status_code = make_query(dataset, queries['detail'])
        if valid_query:
            resp['meta']['status'] = 'ok'
            pk = [p.name for p in dataset.primary_key][0]
            base_query = base_query.join(dataset, mt.c.dataset_row_id == dataset.c[pk])
        for clause in base_clauses:
            base_query = base_query.filter(clause)
        if order_by:
            col, order = order_by.split(',')
            base_query = base_query.order_by(getattr(mt.c[col], order)())
        for clause in detail_clauses:
            base_query = base_query.filter(clause)
        if limit:
            base_query = base_query.limit(limit)
        values = [r for r in base_query.all()]
        fieldnames = dataset.columns.keys()
        for value in values:
            d = {}
            for k,v in zip(fieldnames, value[1:]):
                d[k] = v
            resp['objects'].append(d)
        resp['meta']['total'] = len(resp['objects'])
    if datatype == 'json':
        resp = make_response(json.dumps(resp, default=dthandler), status_code)
        resp.headers['Content-Type'] = 'application/json'
    elif datatype == 'csv':
        csv_resp = [fieldnames]
        csv_resp.extend([v[1:] for v in values])
        resp = make_response(make_csv(csv_resp), 200)
        filedate = datetime.now().strftime('%Y-%m-%d')
        dname = raw_query_params['dataset_name']
        filedate = datetime.now().strftime('%Y-%m-%d')
        resp.headers['Content-Type'] = 'text/csv'
        resp.headers['Content-Disposition'] = 'attachment; filename=%s_%s.csv' % (dname, filedate)
    return resp

@api.route('/api/detail-aggregate/')
@crossdomain(origin="*")
def detail_aggregate():
    raw_query_params = request.args.copy()
    agg, datatype, queries = parse_join_query(raw_query_params)
    mt = MasterTable.__table__
    valid_query, base_clauses, resp, status_code = make_query(mt, queries['base'])
    if valid_query:
        resp['meta']['status'] = 'ok'
        time_agg = func.date_trunc(agg, mt.c['obs_date'])
        base_query = session.query(time_agg, func.count(mt.c.dataset_row_id))
        dname = raw_query_params['dataset_name']
        dataset = Table('dat_%s' % dname, Base.metadata,
            autoload=True, autoload_with=engine,
            extend_existing=True)
        valid_query, detail_clauses, resp, status_code = make_query(dataset, queries['detail'])
        if valid_query:
            resp['meta']['status'] = 'ok'
            pk = [p.name for p in dataset.primary_key][0]
            base_query = base_query.join(dataset, mt.c.dataset_row_id == dataset.c[pk])
            for clause in base_clauses:
                base_query = base_query.filter(clause)
            for clause in detail_clauses:
                base_query = base_query.filter(clause)
            values = [r for r in base_query.group_by(time_agg).order_by(time_agg).all()]
            items = []
            for value in values:
                d = {
                    'group': value[0],
                    'count': value[1]
                }
                items.append(d)
            resp['objects'].append({
                'temporal_aggregate': agg,
                'dataset_name': ' '.join(dname.split('_')).title(),
                'items': items
            })
    resp = make_response(json.dumps(resp, default=dthandler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp

def getSizeInDegrees(meters, latitude):

    earth_circumference = 40041000.0 # meters, average circumference
    degrees_per_meter = 360.0 / earth_circumference
    
    degrees_at_equator = meters * degrees_per_meter

    latitude_correction = 1.0 / math.cos(latitude * (math.pi / 180.0))
    
    degrees_x = degrees_at_equator * latitude_correction
    degrees_y = degrees_at_equator

    return degrees_x, degrees_y

@api.route('/api/grid/')
@crossdomain(origin="*")
def grid():
    dataset_name = request.args.get('dataset_name')
    resolution = request.args.get('resolution')
    obs_to = request.args.get('obs_date__le')
    obs_from = request.args.get('obs_date__ge')
    location_geom = request.args.get('location_geom__within')
    buff = request.args.get('buffer', 100)
    center = request.args.getlist('center[]')
    resp = {'type': 'FeatureCollection', 'features': []}
    size_x, size_y = getSizeInDegrees(float(resolution), float(center[0]))
    if location_geom:
        location_geom = json.loads(location_geom)['geometry']
        if location_geom['type'] == 'LineString':
            shape = asShape(location_geom)
            lat = shape.centroid.y
            # 100 meters by default
            x, y = getSizeInDegrees(int(buff), lat)
            size_x, size_y = getSizeInDegrees(50, lat)
            location_geom = shape.buffer(y).__geo_interface__
        location_geom['crs'] = {"type":"name","properties":{"name":"EPSG:4326"}}
    mt = MasterTable.__table__
    query = session.query(func.count(mt.c.dataset_row_id), 
            func.ST_SnapToGrid(mt.c.location_geom, size_x, size_y))\
            .filter(mt.c.dataset_name == dataset_name)
    if obs_from:
        query = query.filter(mt.c.obs_date >= obs_from)
    if obs_to:
        query = query.filter(mt.c.obs_date <= obs_to)
    if location_geom:
        query = query.filter(mt.c.location_geom\
                .ST_Within(func.ST_GeomFromGeoJSON(json.dumps(location_geom))))
    query = query.group_by(func.ST_SnapToGrid(mt.c.location_geom, size_x, size_y))
    values = [d for d in query.all()]
    for value in values:
        d = {
            'type': 'Feature', 
            'properties': {
                'count': value[0], 
            },
        }
        if value[1]:
            pt = loads(value[1].decode('hex'))
            south, west = (pt.x - (size_x / 2)), (pt.y - (size_y /2))
            north, east = (pt.x + (size_x / 2)), (pt.y + (size_y / 2))
            d['geometry'] = box(south, west, north, east).__geo_interface__
        resp['features'].append(d)
    resp = make_response(json.dumps(resp, default=dthandler))
    resp.headers['Content-Type'] = 'application/json'
    return resp

@api.route('/api/submit-dataset/', methods=['POST'])
def submit_dataset():
    resp = {'status': 'ok', 'message': ''}
    post = request.form
    referer = urlparse(request.headers['Referer']).netloc
    req_url = urlparse(request.url).netloc
    if referer != req_url:
        abort(401)
    if post.get('view_url'):
        dataset_info, errors, status_code = get_socrata_data_info(post['view_url'])
        if errors:
            resp['message'] = ', '.join([e for e in errors])
            resp['status'] = 'error'
            status_code = 400
        else:
            source_domain = urlparse(dataset_info['view_url']).netloc
            dataset_id = dataset_info['view_url'].split('/')[-1]
            source_url = 'http://%s/resource/%s' % (source_domain, dataset_id)
            md = session.query(MetaTable).get(dataset_id)
            if not md:
                d = {
                    'four_by_four': dataset_id,
                    'dataset_name': slugify(dataset_info['name'], delim=u'_'),
                    'human_name': dataset_info['name'],
                    'description': dataset_info['description'],
                    'source_url': source_url,
                    'update_freq': post['update_frequency'],
                    'business_key': post['id_field'],
                    'observed_date': post['date_field'],
                    'latitude': post.get('latitude'),
                    'longitude': post.get('longitude'),
                    'location': post.get('location')
                }
                md = MetaTable(**d)
                session.add(md)
                session.commit()
            add_dataset.delay(md.four_by_four)
            resp['message'] = 'Dataset %s submitted successfully' % md.human_name
    else:
        resp['status'] = 'error'
        resp['message'] = 'Must provide a socrata view url'
        status_code = 400
    resp = make_response(json.dumps(resp, default=dthandler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp

