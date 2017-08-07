import json
import re
from datetime import datetime, timedelta

from flask import jsonify, request
from marshmallow import post_load, pre_dump, Schema
from marshmallow.fields import Int, Str
from marshmallow.validate import OneOf

from plenario.api.common import CACHE_TIMEOUT
from plenario.api.common import cache, crossdomain, make_cache_key
from plenario.api.condition_builder import parse_tree
from plenario.api.point import has_tree_filters, request_args_to_condition_tree
from plenario.api.response import make_error
from plenario.fields import Commalist, DateTime, Geometry, Pointset
from plenario.models import MetaTable


class AggregateValidator(Schema):
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
def aggregate():

    validator = AggregateValidator()
    deserialized_arguments = validator.load(request.args)
    serialized_arguments = json.loads(validator.dumps(deserialized_arguments.data))
    if deserialized_arguments.errors:
        return make_error(deserialized_arguments.errors, 400, serialized_arguments)

    qargs = deserialized_arguments
    start_date = qargs['obs_date__ge']
    end_date = qargs['obs_date__le']
    agg = qargs['agg']
    geom = qargs['location_geom__within']
    dataset = qargs['dataset']

    if not has_tree_filters(qargs):
        # The obs_date arguments set the bounds of all the aggregates.
        # We don't want to create a condition tree that has point_date filters.
        qargs[dataset.name + '__filter'] = request_args_to_condition_tree(
            qargs, ignore=['obs_date__ge', 'obs_date__le']
        )

    results = []
    dataset_conditions = {k: v for k, v in qargs.items() if 'filter' in k}
    for tablename, condition_tree in dataset_conditions.items():
        # This pattern matches the last occurrence of the '__' pattern.
        # Prevents an error that is caused by dataset names with trailing
        # underscores.
        tablename = re.split(r'__(?!_)', tablename)[0]
        table = MetaTable.get_by_dataset_name(tablename).table
        try:
            conditions = parse_tree(table, condition_tree)
        except ValueError:  # Catches empty condition tree.
            conditions = None

        ts = MetaTable.get_by_dataset_name(table.name).timeseries_one(
            agg, start_date, end_date, geom, conditions
        )

        results += [{'count': c, 'datetime': d} for c, d in ts[1:]]

    if datatype == 'csv':
        resp = form_csv_detail_response([], results)
        resp.headers['Content-Type'] = 'text/csv'
        filedate = datetime.now().strftime('%Y-%m-%d')
        resp.headers['Content-Disposition'] = 'attachment; filename=%s.csv' % filedate
    else:
        response = jsonify({
            'meta': {
                'query': qargs
            },

        })
        resp = json_response_base(qargs, results, request.args)
        resp['count'] = sum([c['count'] for c in query_result])
        resp = make_response(json.dumps(resp, default=unknown_object_json_handler), 200)
        resp.headers['Content-Type'] = 'application/json'

    return resp
