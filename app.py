from flask import Flask, make_response, request
from flask.ext.sqlalchemy import SQLAlchemy
import os
from datetime import date
import json
from sqlalchemy import Table, event

app = Flask(__name__)
CONN_STRING = os.environ['EAGER_CONN']
app.config['SQLALCHEMY_DATABASE_URI'] = CONN_STRING

db = SQLAlchemy(app)

class Master(db.Model):
    __table__ = Table('dat_master', db.Model.metadata,
                autoload=True, autoload_with=db.engine)

    def as_dict(row):
        return {c.name: getattr(row, c.name) for c in row.__table__.columns}

dthandler = lambda obj: obj.isoformat() if isinstance(obj, date) else None

@app.route('/')
def meta():
    offset = request.args.get('offset')
    limit = request.args.get('limit')
    if not offset:
        offset = 0
    if not limit:
        limit = 100
    dump = [m.as_dict() for m in Master.query.offset(offset).limit(limit).all()]
    resp = make_response(json.dumps(dump, default=dthandler))
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
    keys = table.columns.keys()
    values = [r for r in db.session.query(table).offset(offset).limit(limit).all()]
    resp = []
    for value in values:
        d = {}
        for k,v in zip(keys, value):
            d[k] = v
        resp.append(d)
    resp = make_response(json.dumps(resp, default=dthandler))
    resp.headers['Content-Type'] = 'application/json'
    return resp

if __name__ == '__main__':
    app.run(debug=True)
