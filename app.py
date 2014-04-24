from flask import Flask, make_response, request, render_template, current_app, g
from functools import update_wrapper
import os
from datetime import date, datetime, timedelta
import time
import json
from sqlalchemy import Table, func, distinct, Column, create_engine, MetaData, Float
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.types import NullType
from sqlalchemy.sql.expression import cast
from geoalchemy2 import Geometry
from operator import itemgetter
from itertools import groupby
from cStringIO import StringIO
import csv

app = Flask(__name__)

engine = create_engine(os.environ['WOPR_CONN'])
metadata = MetaData()
session = scoped_session(sessionmaker(autocommit=False,
                                      autoflush=False,
                                      bind=engine))

dthandler = lambda obj: obj.isoformat() if isinstance(obj, date) else None

@app.teardown_appcontext
def shutdown_session(exception=None):
      session.remove()

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
            val = json.loads(query_value)['geometry']
            val['crs'] = {"type":"name","properties":{"name":"EPSG:4326"}}
            query = column.ST_Within(func.ST_GeomFromGeoJSON(json.dumps(val)))
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

@app.route('/api/')
def meta():
    status_code = 200
    resp = []
    # TODO: Doinaggregate queries here is super slow. It would be nice to speed it up
    # This query only performs well after makinan index on dataset_name
    meta_table = Table('meta_master', metadata,
        autoload=True, autoload_with=engine, extend_existing=True)

    values = session.query(meta_table).all()
    keys = meta_table.columns.keys()
    for value in values:
        d = {}
        for k,v in zip(keys, value):
            d[k] = v
        resp.append(d)
    resp = make_response(json.dumps(resp), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp

@app.route('/api/fields/<dataset_name>/')
@crossdomain(origin="*")
def dataset_fields(dataset_name):
    try:
        table = Table('dat_%s' % dataset_name, metadata,
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

def make_csv(data, fields):
    outp = StringIO()
    writer = csv.DictWriter(outp, fields)
    writer.writeheader()
    writer.writerows(data)
    return outp.getvalue()

@app.route('/api/master/')
@crossdomain(origin="*")
def dataset():
    raw_query_params = request.args.copy()
    agg = raw_query_params.get('agg')
    if not agg:
        # TODO: Make a more informed judment about minumum tempral resolution
        agg = 'day'
    else:
        del raw_query_params['agg']
    datatype = 'json'
    if raw_query_params.get('datatype'):
        datatype = raw_query_params['datatype']
        del raw_query_params['datatype']
    master_table = Table('dat_master', metadata,
        autoload=True, autoload_with=engine, extend_existing=True)
    valid_query, query_clauses, resp, status_code = make_query(master_table,raw_query_params)
    if valid_query:
        time_agg = func.date_trunc(agg, master_table.c['obs_date'])
        base_query = session.query(time_agg, 
            func.count(master_table.c['obs_date']),
            master_table.c['dataset_name'])
        base_query = base_query.filter(master_table.c['current_flag'] == True)
        for clause in query_clauses:
            base_query = base_query.filter(clause)
        base_query = base_query.group_by(master_table.c['dataset_name'])\
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
            d = {'dataset_name': ' '.join(k.split('_')).title()}
            d['temporal_aggregate'] = agg
            d['items'] = list(g)
            resp['objects'].append(d)
        resp['meta']['status'] = 'ok'
    if datatype == 'json':
        resp = make_response(json.dumps(resp, default=dthandler), status_code)
        resp.headers['Content-Type'] = 'application/json'
    elif datatype == 'csv':
        if not raw_query_params.get('dataset_name'):
            resp = {
                'meta': {
                    'status': 'error',
                    'message': 'If you want data in a CSV format, you also need to specify a dataset_name'
                },
                'objects': []
            }
        else:
            data = resp['objects'][0]
            fields = data['items'][0].keys()
            resp = make_response(make_csv(data['items'], fields), 200)
            resp.headers['Content-Type'] = 'text/csv'
            dname = raw_query_params['dataset_name']
            filedate = datetime.now().strftime('%Y-%m-%d')
            resp.headers['Content-Disposition'] = 'attachment; filename=%s_%s.csv' % (dname, filedate)
    return resp

def parse_join_query(params):
    queries = {
        'base' : {},
        'detail': {},
    }
    agg = 'day'
    datatype = 'json'
    for key, value in params.items():
        if key.split('__')[0] in ['obs_date', 'geom', 'dataset_name']:
            queries['base'][key] = value
        elif key == 'agg':
            agg = value
        elif key == 'datatype':
            datatype = value
        else:
            queries['detail'][key] = value
    return agg, datatype, queries

@app.route('/api/detail/')
@crossdomain(origin="*")
def detail():
    raw_query_params = request.args.copy()
    agg, datatype, queries = parse_join_query(raw_query_params)
    master_table = Table('dat_master', metadata,
        autoload=True, autoload_with=engine, extend_existing=True)
    valid_query, base_clauses, resp, status_code = make_query(master_table, queries['base'])
    if valid_query:
        resp['meta']['status'] = 'ok'
        dname = raw_query_params['dataset_name']
        dataset = Table('dat_%s' % dname, metadata,
            autoload=True, autoload_with=engine,
            extend_existing=True)
        base_query = session.query(master_table.c.obs_date, dataset)
        valid_query, detail_clauses, resp, status_code = make_query(dataset, queries['detail'])
        if valid_query:
            resp['meta']['status'] = 'ok'
            pk = [p.name for p in dataset.primary_key][0]
            base_query = base_query.join(dataset, master_table.c.dataset_row_id == dataset.c[pk])
        for clause in base_clauses:
            base_query = base_query.filter(clause)
        for clause in detail_clauses:
            base_query = base_query.filter(clause)
        values = [r for r in base_query.order_by(master_table.c.obs_date.desc()).all()]
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
        resp = make_response(make_csv(resp['objects'], fieldnames), 200)
        filedate = datetime.now().strftime('%Y-%m-%d')
        dname = raw_query_params['dataset_name']
        filedate = datetime.now().strftime('%Y-%m-%d')
        resp.headers['Content-Type'] = 'text/csv'
        resp.headers['Content-Disposition'] = 'attachment; filename=%s_%s.csv' % (dname, filedate)
    return resp

@app.route('/api/detail-aggregate/')
@crossdomain(origin="*")
def detail_aggregate():
    raw_query_params = request.args.copy()
    agg, datatype, queries = parse_join_query(raw_query_params)
    master_table = Table('dat_master', metadata,
        autoload=True, autoload_with=engine, extend_existing=True)
    valid_query, base_clauses, resp, status_code = make_query(master_table, queries['base'])
    if valid_query:
        resp['meta']['status'] = 'ok'
        time_agg = func.date_trunc(agg, master_table.c['obs_date'])
        base_query = session.query(time_agg, func.count(master_table.c.dataset_row_id))
        dname = raw_query_params['dataset_name']
        dataset = Table('dat_%s' % dname, metadata,
            autoload=True, autoload_with=engine,
            extend_existing=True)
        valid_query, detail_clauses, resp, status_code = make_query(dataset, queries['detail'])
        if valid_query:
            resp['meta']['status'] = 'ok'
            pk = [p.name for p in dataset.primary_key][0]
            base_query = base_query.join(dataset, master_table.c.dataset_row_id == dataset.c[pk])
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

@app.route('/')
def index():
    return render_app_template('index.html')

# UTILITY
def render_app_template(template, **kwargs):
    '''Add some goodies to all templates.'''

    if 'config' not in kwargs:
        kwargs['config'] = app.config
    return render_template(template, **kwargs)

if __name__ == '__main__':
    app.run(debug=True)
