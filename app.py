from flask import Flask, make_response, request, render_template, current_app
from functools import update_wrapper
from flask.ext.sqlalchemy import SQLAlchemy
import os
from datetime import date, datetime, timedelta
import time
import json
from sqlalchemy import Table, func, distinct, Column
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.types import NullType
from geoalchemy2 import Geometry
from operator import itemgetter
from itertools import groupby
from cStringIO import StringIO
import csv

app = Flask(__name__)
CONN_STRING = os.environ['WOPR_CONN']
app.config['SQLALCHEMY_DATABASE_URI'] = CONN_STRING

db = SQLAlchemy(app)

dthandler = lambda obj: obj.isoformat() if isinstance(obj, date) else None
master_table = Table('dat_master', db.Model.metadata,
        autoload=True, autoload_with=db.engine)

def crossdomain(origin=None, methods=None, headers=None,
                max_age=21600, attach_to_all=True,
                automatic_options=True):
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
            if query_value == 'null':
                query_value = None
            query = getattr(column, attr)(query_value)
            query_clauses.append(query)
    return valid_query, query_clauses, resp, status_code

@app.route('/api/')
def meta():
    status_code = 200
    resp = []
    # TODO: Doing aggregate queries here is super slow. It would be nice to speed it up
    # This query only performs well after making an index on dataset_name
    values = db.session.query(
        distinct(master_table.columns.get('dataset_name'))).all()
    for value in values:
       #obs_to, obs_from = (value[1].strftime('%Y-%m-%d'), value[2].strftime('%Y-%m-%d'))
       #observed_range = '%s - %s' % (obs_from, obs_to)
       #s = select([func.ST_AsGeoJSON(func.ST_Estimated_Extent(
       #    'dat_%s' % value[0], 'geom'))])
       #bbox = json.loads(list(db.engine.execute(s))[0][0])
        d = {
            'machine_name': value[0],
            'human_name': ' '.join(value[0].split('_')).title(),
           #'observed_date_range': observed_range,
           #'bounding_box': bbox,
        }
        resp.append(d)
    resp = make_response(json.dumps(resp), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp

@app.route('/api/fields/<dataset_name>/')
@crossdomain(origin="*")
def dataset_fields(dataset_name):
    table = Table('dat_%s' % dataset_name, db.Model.metadata,
        autoload=True, autoload_with=db.engine,
        extend_existing=True)
    fields = table.columns.keys()
    data = []
    for col in table.columns:
        if not isinstance(col.type, NullType):
            d = {}
            d['field_name'] = col.name
            d['field_type'] = str(col.type)
            data.append(d)
    resp = make_response(json.dumps(data))
    resp.headers['Content-Type'] = 'application/json'
    return resp

@app.route('/api/master/')
@crossdomain(origin="*")
def dataset():
    raw_query_params = request.args.copy()
    agg = raw_query_params.get('agg')
    if not agg:
        # TODO: Make a more informed judgement about minumum tempral resolution
        agg = 'day'
    else:
        del raw_query_params['agg']
    datatype = 'json'
    if raw_query_params.get('datatype'):
        datatype = raw_query_params['datatype']
        del raw_query_params['datatype']
    valid_query, query_clauses, resp, status_code = make_query(master_table,raw_query_params)
    if valid_query:
        time_agg = func.date_trunc(agg, master_table.c['obs_date'])
        base_query = db.session.query(time_agg, 
            func.count(master_table.c['obs_date']),
            master_table.c['dataset_name'])
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
            d['objects'] = list(g)
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
            outp = StringIO()
            data = resp['objects'][0]
            fields = data['objects'][0].keys()
            writer = csv.DictWriter(outp, fields)
            writer.writeheader()
            writer.writerows(data['objects'])
            resp = make_response(outp.getvalue(), 200)
            resp.headers['Content-Type'] = 'text/csv'
    return resp

@app.route('/api/detail-aggregate/')
@crossdomain(origin="*")
def details():
    raw_query_params = request.args.copy()
    agg = raw_query_params.get('base-agg')
    if not agg:
        # TODO: Make a more informed judgement about minumum tempral resolution
        agg = 'day'
    else:
        del raw_query_params['base-agg']
    datatype = 'json'
    if raw_query_params.get('base-datatype'):
        datatype = raw_query_params['base-datatype']
        del raw_query_params['base-datatype']
    queries = {
        'base' : {},
        'detail': {},
    }
    for k,v in raw_query_params.items():
        qt, field = k.split('-')
        queries[qt][field] = v
    valid_query, base_clauses, resp, status_code = make_query(master_table, queries['base'])
    if valid_query:
        resp['meta']['status'] = 'ok'
        time_agg = func.date_trunc(agg, master_table.c['obs_date'])
        base_query = db.session.query(time_agg, func.count(master_table.c.dataset_row_id))
        dataset = Table('dat_%s' % raw_query_params['base-dataset_name'], db.Model.metadata,
            autoload=True, autoload_with=db.engine,
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
        for value in values:
            d = {
                'group': value[0],
                'count': value[1]
            }
            resp['objects'].append(d)
    resp = make_response(json.dumps(resp, default=dthandler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp

@app.route('/')
def index():
    return render_app_template('index.html')

@app.route('/map/')
def map():
    return render_app_template('map.html')

# UTILITY
def render_app_template(template, **kwargs):
    '''Add some goodies to all templates.'''

    if 'config' not in kwargs:
        kwargs['config'] = app.config
    return render_template(template, **kwargs)

if __name__ == '__main__':
    app.run(debug=True)
