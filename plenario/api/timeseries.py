import json
import re

from datetime import datetime, timedelta
from flask import request, jsonify
from marshmallow import Schema
from marshmallow.decorators import post_load
from marshmallow.fields import Str, List
from marshmallow.validate import OneOf

from plenario.api.common import crossdomain, cache, CACHE_TIMEOUT, make_cache_key
from plenario.api.condition_builder import parse_tree
from plenario.api.fields import Geometry, Pointset, DateTime
from plenario.api.response import make_error
from plenario.api.validator import has_tree_filters
from plenario.models import MetaTable


class TimeseriesValidator(Schema):
    agg = Str(default='week', validate=OneOf({'day', 'week', 'month', 'quarter', 'year'}))
    dataset_name = Pointset()
    dataset_name__in = List(Pointset, default=lambda: list())
    location_geom__within = Geometry(default=None)
    obs_date__ge = DateTime(default=lambda: datetime.now())
    obs_date__le = DateTime(default=lambda: datetime.now() - timedelta(days=90))
    data_type = Str(default='json', validate=OneOf({'csv', 'json'}))

    @post_load
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
def timeseries():
    validator = TimeseriesValidator()
    deserialized_arguments = validator.load(request.args)

    if deserialized_arguments.errors:
        return make_error(deserialized_arguments.error, 400, deserialized_arguments.data)

    qargs = deserialized_arguments.data

    agg = qargs['agg']
    geom = qargs['location_geom__within']
    pointset = [qargs['dataset_name']]
    pointsets = qargs['dataset_name__in']
    start_date = qargs['obs_date__ge']
    end_date = qargs['obs_date__le']

    ctrees = {}

    if has_tree_filters(qargs):
        # Timeseries is a little tricky. If there aren't filters,
        # it would be ridiculous to build a condition tree for every one.
        for field, value in list(qargs.items()):
            if 'filter' in field:
                # This pattern matches the last occurrence of the '__' pattern.
                # Prevents an error that is caused by dataset names with trailing
                # underscores.
                tablename = re.split(r'__(?!_)', field)[0]
                metarecord = MetaTable.get_by_dataset_name(tablename)
                pt = metarecord.point_table
                ctrees[pt.name] = parse_tree(pt, value)

    point_set_names = [p.name for p in pointsets + pointset]
    results = MetaTable.timeseries_all(point_set_names, agg, start_date, end_date, geom, ctrees)

    payload = {
        'meta': {
            'message': [],
            'query': json.loads(validator.dumps(deserialized_arguments.data).data),
            'status': 'ok',
            'total': len(results)
        },
        'objects': results
    }

    return jsonify(payload)
