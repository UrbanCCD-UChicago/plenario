from flask import Flask, make_response, request, render_template
from flask.ext.sqlalchemy import SQLAlchemy
import os
from datetime import date
import json
from sqlalchemy import Table, func

app = Flask(__name__)
CONN_STRING = os.environ['WOPR_CONN']
app.config['SQLALCHEMY_DATABASE_URI'] = CONN_STRING

db = SQLAlchemy(app)

dthandler = lambda obj: obj.isoformat() if isinstance(obj, date) else None

OPERATORS = {
    'eq': '=',
    'lt': '<',
    'lte': '<=',
    'gt': '>',
    'gte': '>=',
}

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
            value = raw_query_params.get(query_param)
            query = column.in_(value.split(','))
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
    table = Table('dat_master', db.Model.metadata,
            autoload=True, autoload_with=db.engine)
    resp = []
    values = db.session.query(
        table.columns.get('dataset_name'),
        func.max(table.columns.get('obs_date')),
        func.min(table.columns.get('obs_date')),
        func.max(table.columns.get('longitude')),
        func.min(table.columns.get('longitude')),
        func.max(table.columns.get('latitude')),
        func.min(table.columns.get('latitude')))\
        .group_by('dataset_name').all()
    for value in values:
        obs_to, obs_from = (value[1].strftime('%Y-%m-%d'), value[2].strftime('%Y-%m-%d'))
        observed_range = '%s - %s' % (obs_from, obs_to)
        sw = (value[3], value[5])
        ne = (value[4], value[6])
        d = {
            'machine_name': value[0],
            'human_name': ' '.join(value[0].split('_')).title(),
            'observed_date_range': observed_range,
            'bounding_box': (sw, ne),
        }
        resp.append(d)
    resp = make_response(json.dumps(resp, default=dthandler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp

@app.route('/api/<dataset>/')
def dataset(dataset):
    offset = request.args.get('offset')
    limit = request.args.get('limit')
    if not offset:
        offset = 0
    if not limit:
        limit = 100
    table = Table('dat_%s' % dataset, db.Model.metadata,
            autoload=True, autoload_with=db.engine)
    table_keys = table.columns.keys()
    raw_query_params = request.args.copy()
    valid_query, query_clauses, resp, status_code = make_query(table,raw_query_params)
    if valid_query:
        resp['meta']['status'] = 'ok'
        resp['meta']['message'] = None
        base_query = db.session.query(table)
        for clause in query_clauses:
            base_query = base_query.filter(clause)
        values = [r for r in base_query.offset(offset).limit(limit).all()]
        for value in values:
            d = {}
            for k,v in zip(table_keys, value):
                d[k] = v
            resp['objects'].append(d)
    resp = make_response(json.dumps(resp, default=dthandler))
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
