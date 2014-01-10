from flask import Flask, make_response, request, render_template
from flask.ext.sqlalchemy import SQLAlchemy
import os
from datetime import date, datetime, timedelta
import time
import json
from sqlalchemy import Table, func, distinct, Column
from sqlalchemy.exc import NoSuchTableError
from geoalchemy2 import Geometry
from operator import itemgetter
from itertools import groupby

app = Flask(__name__)
CONN_STRING = os.environ['WOPR_CONN']
app.config['SQLALCHEMY_DATABASE_URI'] = CONN_STRING

db = SQLAlchemy(app)

dthandler = lambda obj: obj.isoformat() if isinstance(obj, date) else None
master_table = Table('dat_master', db.Model.metadata,
        autoload=True, autoload_with=db.engine)

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

@app.route('/api/<agg>/')
def dataset(agg):
    resp = {
        'meta': {
            'status': 'error',
            'message': '',
        },
        'objects': [],
    }
    status_code = 200
    raw_query_params = request.args.copy()
    valid_query, query_clauses, resp, status_code = make_query(master_table,raw_query_params)
    if valid_query:
        start_ts = request.args.get('start_time')
        end_ts = request.args.get('end_time')
        now = datetime.now()
        if not start_ts:
            then = now - timedelta(days=180)
            start_ts = time.mktime(then.timetuple())
        if not end_ts:
            end_ts = time.mktime(now.timetuple())
        start_time = datetime.fromtimestamp(float(start_ts))
        end_time = datetime.fromtimestamp(float(end_ts))
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
        for value in values:
            d = {
                'dataset_name': value[2],
                'temporal_group': value[0],
                'count': value[1],
                }
            resp['objects'].append(d)
        resp['meta']['status'] = 'ok'
    resp = make_response(json.dumps(resp, default=dthandler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp

@app.route('/api/master/')
def master():
    offset = request.args.get('offset')
    limit = request.args.get('limit')
    if not offset:
        offset = 0
    if not limit:
        limit = 500
    status_code = 200
    table_keys = master_table.columns.keys()
    raw_query_params = request.args.copy()
    valid_query, query_clauses, resp, status_code = make_query(master_table,raw_query_params)
    if valid_query:
        resp['meta']['status'] = 'ok'
        resp['meta']['message'] = None
        base_query = db.session.query(master_table.c.dataset_name,\
            master_table.c.dataset_row_id, func.ST_AsGeoJSON(master_table.c.geom))
        for clause in query_clauses:
            base_query = base_query.filter(clause)
        values = [r for r in base_query.offset(offset).limit(limit).all()]
        results = []
        for value in values:
            d = {
                'dataset_name': value[0],
                'dataset_row_id': value[1],
                'geom': json.loads(value[2]),
                }
            results.append(d)
        results = sorted(results, key=itemgetter('dataset_name'))
        ids = {}
        for k,g in groupby(results, key=itemgetter('dataset_name')):
            group = list(g)
            ids[k] = [i['dataset_row_id'] for i in group]
            resp[k] = {}
            for item in group:
                resp[k][item['dataset_row_id']] = {'geom': item['geom']}
        for name,pks in ids.items():
            dataset = Table('dat_%s' % name, db.Model.metadata,
                autoload=True, autoload_with=db.engine,
                extend_existing=True)
            pk = [p for p in dataset.primary_key][0]
            set_keys = dataset.columns.keys()
            details = [d for d in db.session.query(dataset).filter(pk.in_(pks)).all()]
            for detail in details:
                d = {}
                for k,v in zip(set_keys, detail):
                    d[k] = v
                resp[name][d[pk.name]] = d
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
