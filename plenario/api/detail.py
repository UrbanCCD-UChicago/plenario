import json
from datetime import datetime, timedelta

from flask import request
from marshmallow import post_load, pre_dump, Schema
from marshmallow.fields import Int, Str
from marshmallow.validate import OneOf

from plenario.api.common import CACHE_TIMEOUT
from plenario.api.common import cache, crossdomain, make_cache_key
from plenario.api.point import detail_query
from plenario.fields import Commalist, DateTime, Geometry, Pointset


class DetailValidator(Schema):
    data_type = Str(default='json', validate=OneOf({'csv', 'json'}))
    limit = Int(default=1000)
    dataset_name = Pointset(default=None)
    dataset_name__in = Commalist(Pointset(), default=lambda: list())
    location_geom__within = Geometry(default=None)
    offset = Int(default=0)
    obs_date__ge = DateTime(default=lambda: datetime.now() - timedelta(days=90))
    obs_date__le = DateTime(default=lambda: datetime.now())

    @post_load
    def defaults(self, data):
        for name, field in self.fields.items():
            if name not in data:
                if callable(field.default):
                    data[name] = field.default()
                else:
                    data[name] = field.default
        return data

    @pre_dump
    def defaults(self, data):
        for name, field in self.fields.items():
            if name not in data:
                if callable(field.default):
                    data[name] = field.default()
                else:
                    data[name] = field.default
        return data


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin='*')
def detail():
    validator = DetailValidator()
    deserialized_arguments = validator.load(request.args)
    serialized_arguments = json.loads(validator.dumps(deserialized_arguments.data))
    if deserialized_arguments.errors:
        return api_response.bad_request(deserialized_arguments.errors)

    qargs = deserialized_arguments

    dataset = qargs['dataset']
    data_type = qargs['data_type']
    limit = qargs['limit']
    offset = qargs['offset']

    query = detail_query(qargs).order_by(dataset.c.point_date.desc())
    query = query.limit(limit)
    query = query.offset(offset) if offset else query

    import pdb
    pdb.set_trace()

    results = [row for row in query]

    to_remove = ['point_date', 'hash']

    payload = {
        'meta': {
            'message': [],
            'query': serialized_arguments,
            'status': 'ok',
            'total': len(results)
        },
        'objects': results
    }

    if data_type == 'json':
        return form_json_detail_response(to_remove, qargs, results)

    elif data_type == 'csv':
        return form_csv_detail_response(to_remove, results)

    elif data_type == 'geojson':
        return form_geojson_detail_response(to_remove, results)
