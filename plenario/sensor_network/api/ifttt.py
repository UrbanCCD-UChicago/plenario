import json
from datetime import datetime, timedelta

from flask import request, make_response
from sqlalchemy import MetaData, Table

from plenario.database import redshift_session, redshift_engine
from plenario.api.common import crossdomain
from plenario.api.common import unknown_object_json_handler


@crossdomain(origin="*")
def property_comparison():
    args = request.json
    # map to curated features
    if args.triggerFields.prop == "temperature":
        args.triggerFields.feature = "temperature"
        args.triggerFields.property = "temperature"
    print comparison_query(args).all()


def comparison_query(args):
    node = args.triggerFields.node
    op = args.triggerFields.op
    val = args.triggerFields.val
    feature = args.triggerFields.feature
    property = args.triggerFields.property

    limit = None
    if args.limit:
        limit = args.limit

    meta = MetaData()
    table = Table(
        feature, meta,
        autoload=True,
        autoload_with=redshift_engine
    )
    q = redshift_session.query(table)
    q = q.filter(table.c.datetime >= (datetime.utcnow() - timedelta(minutes=30)))
    q = q.filter(table.c.node == node)

    if op == 'gt':
        q = q.filter(table.c[property] > val)
    elif op == 'ge':
        q = q.filter(table.c[property] >= val)
    elif op == 'lt':
        q = q.filter(table.c[property] < val)
    elif op == 'le':
        q = q.filter(table.c[property] <= val)
    else:
        q = q.filter(table.c[property] == val)

    q = q.limit(limit) if limit else q

    return q


def format_response():
    resp = {
        'data': []
    }
    resp = make_response(json.dumps(resp, default=unknown_object_json_handler), 200)
    resp.headers['Content-Type'] = 'application/json'
    return resp


property_comparison(
    {"triggerFields":
        {
            "prop": "temperature",
            "op": "gt",
            "val": 70
        },
        "limit": 10
    })
