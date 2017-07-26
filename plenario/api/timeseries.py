import json
import re

from datetime import datetime, timedelta
from flask import request, jsonify
from itertools import groupby
from operator import itemgetter
from marshmallow import Schema
from marshmallow.decorators import pre_dump, post_load
from marshmallow.fields import Str, List
from marshmallow.validate import OneOf

from plenario.api.common import crossdomain, cache, CACHE_TIMEOUT, make_cache_key
from plenario.api.condition_builder import parse_tree
from plenario.api.fields import Geometry, Pointset, DateTime, Commalist
from plenario.api.response import make_error, make_csv, make_response
from plenario.api.validator import has_tree_filters
from plenario.models import MetaTable


class TimeseriesValidator(Schema):
    agg = Str(default='week', validate=OneOf({'day', 'week', 'month', 'quarter', 'year'}))
    dataset_name = Pointset(default=None)
    dataset_name__in = Commalist(Pointset(), default=lambda: list())
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
def timeseries():
    validator = TimeseriesValidator()

    deserialized_arguments = validator.load(request.args)
    serialized_arguments = json.loads(validator.dumps(deserialized_arguments.data).data)

    if deserialized_arguments.errors:
        return make_error(deserialized_arguments.errors, 400, serialized_arguments)

    qargs = deserialized_arguments.data

    agg = qargs['agg']
    data_type = qargs['data_type']
    geom = qargs['location_geom__within']
    pointset = qargs['dataset_name']
    pointsets = qargs['dataset_name__in']
    start_date = qargs['obs_date__ge']
    end_date = qargs['obs_date__le']

    ctrees = {}
    raw_ctrees = {}

    if has_tree_filters(request.args):
        # Timeseries is a little tricky. If there aren't filters,
        # it would be ridiculous to build a condition tree for every one.
        for field, value in list(request.args.items()):
            if 'filter' in field:
                # This pattern matches the last occurrence of the '__' pattern.
                # Prevents an error that is caused by dataset names with trailing
                # underscores.
                tablename = re.split(r'__(?!_)', field)[0]
                metarecord = MetaTable.get_by_dataset_name(tablename)
                pt = metarecord.point_table
                ctrees[pt.name] = parse_tree(pt, json.loads(value))
                raw_ctrees[pt.name] = json.loads(value)

    point_set_names = [p.name for p in pointsets + [pointset] if p is not None]
    if not point_set_names:
        point_set_names = MetaTable.index()

    results = MetaTable.timeseries_all(point_set_names, agg, start_date, end_date, geom, ctrees)

    payload = {
        'meta': {
            'message': [],
            'query': serialized_arguments,
            'status': 'ok',
            'total': len(results)
        },
        'objects': results
    }

    if ctrees:
        payload['meta']['query']['filters'] = raw_ctrees

    if data_type == 'json':
        return jsonify(payload)

    elif data_type == 'csv':

        # response format
        # temporal_group,dataset_name_1,dataset_name_2
        # 2014-02-24 00:00:00,235,653
        # 2014-03-03 00:00:00,156,624

        fields = ['temporal_group']
        for o in payload['objects']:
            fields.append(o['dataset_name'])

        csv_resp = []
        i = 0
        for k, g in groupby(payload['objects'], key=itemgetter('dataset_name')):
            l_g = list(g)[0]

            j = 0
            for row in l_g['items']:
                # first iteration, populate the first column with temporal_groups
                if i == 0:
                    csv_resp.append([row['datetime']])
                csv_resp[j].append(row['count'])
                j += 1
            i += 1

        csv_resp.insert(0, fields)
        csv_resp = make_csv(csv_resp)
        resp = make_response(csv_resp, 200)
        resp.headers['Content-Type'] = 'text/csv'
        filedate = datetime.now().strftime('%Y-%m-%d')
        resp.headers['Content-Disposition'] = 'attachment; filename=%s.csv' % filedate

        return resp
