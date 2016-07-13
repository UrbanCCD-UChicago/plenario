import json
import shapely.geometry
import sqlalchemy

from collections import OrderedDict
from datetime import datetime
from flask import request, make_response
from itertools import groupby
from operator import itemgetter
from shapely.geometry import mapping
from shapely import wkb
from geoalchemy2 import Geometry

from plenario.api.common import cache, crossdomain, CACHE_TIMEOUT
from plenario.api.common import make_cache_key, date_json_handler, unknown_object_json_handler
from plenario.api.condition_builder import parse_tree
from plenario.api.sensor_response import json_response_base
from plenario.api.response import internal_error, bad_request, make_csv
from plenario.api.response import geojson_response_base, form_csv_detail_response, form_json_detail_response
from plenario.api.response import form_geojson_detail_response, add_geojson_feature
from plenario.api.validator import DatasetRequiredValidator, NoGeoJSONDatasetRequiredValidator
from plenario.api.validator import NoDefaultDatesValidator, validate, NoGeoJSONValidator, has_tree_filters
from plenario.database import session
from plenario.sensor_models import NetworkMeta, NodeMeta


# @cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def get_all_sensor_network_metadata():
    q = session.query(NetworkMeta)
    data = [_format_network_metadata(network) for network in q.all()]

    status_code = 200
    resp = json_response_base(data)
    resp = make_response(json.dumps(resp), status_code)
    resp.headers['Content-Type'] = 'application/json'

    return resp


# @cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def get_sensor_network_metadata(network_name):
    q = session.query(NetworkMeta)
    data = [_format_network_metadata(network) for network in q.all() if network.name == network_name]

    status_code = 200
    resp = json_response_base(data)
    resp = make_response(json.dumps(resp), status_code)
    resp.headers['Content-Type'] = 'application/json'

    return resp


# @cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def get_all_sensor_node_metadata(network_name):
    q = session.query(NodeMeta)
    data = [_format_node_metadata(node) for node in q.all() if node.sensorNetwork == network_name]

    status_code = 200
    resp = json_response_base(data)
    resp = make_response(json.dumps(resp), status_code)
    resp.headers['Content-Type'] = 'application/json'

    return resp


# @cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def get_sensor_node_metadata(node_id, network_name=None):
    q = session.query(NodeMeta)
    data = [_format_node_metadata(node) for node in q.all() if node.id == node_id]

    status_code = 200
    resp = json_response_base(data)
    resp = make_response(json.dumps(resp), status_code)
    resp.headers['Content-Type'] = 'application/json'

    return resp

def _format_network_metadata(network):
    network_response = {
        'name': network.name,
        'nodeMetadata': network.nodeMetadata,
        'nodes': [node.id for node in network.nodes],
        'featuresOfInterest': network.featuresOfInterest
    }

    return network_response


def _format_node_metadata(node):
    node_response = {
        'id': node.id,
        'sensorNetwork': node.sensorNetwork,
        'location': {
            'lat': wkb.loads(bytes(node.location.data)).y,
            'lon': wkb.loads(bytes(node.location.data)).x
        },
        'version': node.version,
        'featuresOfInterest': node.featuresOfInterest,
        'procedures': node.procedures
    }

    return node_response


