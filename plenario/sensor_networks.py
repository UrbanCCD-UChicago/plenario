import json
import shapely.geometry
import shapely.wkb
import sqlalchemy

from collections import OrderedDict
from datetime import datetime
from flask import request, make_response
from itertools import groupby
from operator import itemgetter

from plenario.api.common import cache, crossdomain, CACHE_TIMEOUT
from plenario.api.common import make_cache_key, date_json_handler, unknown_object_json_handler
from plenario.api.condition_builder import parse_tree
from plenario.api.response import internal_error, bad_request, json_response_base, make_csv
from plenario.api.response import geojson_response_base, form_csv_detail_response, form_json_detail_response
from plenario.api.response import form_geojson_detail_response, add_geojson_feature
from plenario.api.validator import DatasetRequiredValidator, NoGeoJSONDatasetRequiredValidator
from plenario.api.validator import NoDefaultDatesValidator, validate, NoGeoJSONValidator, has_tree_filters
from plenario.database import session
from plenario.sensor_models import NetworkMeta

# @cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def all_sensor_network_metadata():

    cols_to_return = [col for col in dir(NetworkMeta) if not col.startswith('__') and not callable(getattr(NetworkMeta, col))]

    resp = {
            'meta': {
                'status': 'ok',
                'message': '',
            },
            'objects': []
        }

    q = session.query(NetworkMeta)
    for network in q.all():
        network_response = {'name': network.name,
                 'nodeMetadata': network.nodeMetadata,
                 'nodes': [node.id for node in network.nodes],
                 'featuresOfInterest': network.featuresOfInterest}
        resp['objects'].append(network_response)
    status_code = 200

    resp['meta']['total'] = len(resp['objects'])
    resp = make_response(json.dumps(resp), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp
