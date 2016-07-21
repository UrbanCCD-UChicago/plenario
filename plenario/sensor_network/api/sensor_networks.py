import json
import shapely.geometry
import sqlalchemy
import ast
import dateutil.parser

from collections import OrderedDict
from datetime import datetime
from flask import request, make_response, Response
from itertools import groupby
from operator import itemgetter
from shapely.geometry import mapping
from shapely import wkb
from geoalchemy2 import Geometry

from plenario.api.common import cache, crossdomain, CACHE_TIMEOUT
from plenario.api.common import make_cache_key, date_json_handler, unknown_object_json_handler
from plenario.api.condition_builder import parse_tree
from plenario.sensor_network.api.sensor_response import json_response_base, bad_request
from plenario.sensor_network.api.sensor_validator import SensorNetworkValidator, validate
from plenario.database import session, redshift_session
from plenario.sensor_network.sensor_models import NetworkMeta, NodeMeta, Observation
from plenario.sensor_network import dynamodb_query


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def get_network_metadata(network_name=None):
    fields = ('network_name',)

    if network_name:
        args = {'network_name': network_name}
        validator = SensorNetworkValidator(only=fields)
        validated_args = validate(validator, args)
        if validated_args.errors:
            return bad_request(validated_args.errors)

        return _get_network_metadata(validated_args.data.get('network_name'))
    else:
        return _get_network_metadata()


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def get_node_metadata(node_id=None, network_name=None):
    fields = ('network_name', 'node_id', 'location_geom__within')

    args = request.args.to_dict()
    if network_name:
        args['network_name'] = network_name
    if node_id:
        args['node_id'] = node_id

    validator = SensorNetworkValidator(only=fields)
    validated_args = validate(validator, args)
    if validated_args.errors:
        return bad_request(validated_args.errors)

    return _get_node_metadata(validated_args)

@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def get_observations(network_name, node_id=None):
    fields = ('network_name', 'node_id', 'nodes',
              'start_datetime', 'end_datetime',
              'location_geom__within', 'filter'
              )

    args = request.args.to_dict()
    args['network_name'] = network_name
    if node_id:
        args['node_id'] = node_id
    elif 'nodes' in args.keys():
        args['nodes'] = ast.literal_eval(args.get('nodes'))

    if 'nodes' in args.keys() and 'node_id' in args.keys():
        return bad_request("Cannot specify single node ID and nodes filter")
    # do we want to allow single node ID and geom ?
    # if 'location_geom__within' in args.keys() and 'node_id' in args.keys():
    #     return bad_request("Cannot specify single node ID and geom filter")

    if 'filter' in args.keys():
        args['filter'] = ast.literal_eval(args.get('filter'))

    validator = SensorNetworkValidator(only=fields)
    validated_args = validate(validator, args)
    if validated_args.errors:
        return bad_request(validated_args.errors)

    return _get_observations(validated_args)


def node_metadata_query(args):
    params = ('network_name', 'node_id', 'nodes',
              'geom', 'filter',
              'start_datetime', 'end_datetime')
    vals = (args.data.get(k) for k in params)
    network_name, node_id, nodes, geom, filter, start_datetime, end_datetime = vals

    q = session.query(NodeMeta)

    # if path specified a sensor network or node ID, filter results accordingly
    if node_id:
        q = q.filter(NodeMeta.id == node_id)
    if network_name:
        q = q.filter(NodeMeta.sensorNetwork == network_name)

    # if the user specified a node ID list, filter to those nodes
    if nodes:
        q = q.filter(NodeMeta.id.in_(nodes))

    # If the user specified a geom, filter results to those within its shape.
    if geom:
        q = q.filter(NodeMeta.location.ST_Within(
            sqlalchemy.func.ST_GeomFromGeoJSON(geom)
        ))

    return q


def format_network_metadata(network):
    network_response = {
        'name': network.name,
        'nodeMetadata': network.nodeMetadata,
        'nodes': [node.id for node in network.nodes],
        'featuresOfInterest': network.featuresOfInterest
    }

    return network_response


def format_node_metadata(node):
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


def _get_network_metadata(network_name=None):

    q = session.query(NetworkMeta)
    data = [format_network_metadata(network) for network in q.all()
            if network.name == network_name or network_name is None]

    # should use validator?
    # json_response_base() in sensor_response.py needs validator for warnings?
    resp = json_response_base(data)
    resp = make_response(json.dumps(resp), 200)
    resp.headers['Content-Type'] = 'application/json'

    return resp


def _get_node_metadata(args):

    q = node_metadata_query(args)
    data = [format_node_metadata(node) for node in q.all()]

    # should use validator?
    # json_response_base() in sensor_response.py needs validator for warnings?
    resp = json_response_base(data)
    resp = make_response(json.dumps(resp), 200)
    resp.headers['Content-Type'] = 'application/json'

    return resp


def _get_observations(args):

    if type(node_metadata_query(args)) is Response:  # better way to catch bad_request?
        return node_metadata_query(args)
    else:
        nodes_to_query = [node.id for node in node_metadata_query(args).all()]

    args.data['nodes'] = nodes_to_query

    # convert datetime into ISO string for dynamodb queries
    if type(args.data.get('start_datetime')) is datetime:
        args.data['start_datetime'] = args.data['start_datetime'].isoformat().split('+')[0]
    if type(args.data.get('end_datetime')) is datetime:
        args.data['end_datetime'] = args.data['end_datetime'].isoformat().split('+')[0]

    # 'node_id'and 'location_geom__within' are now encapsulated within 'nodes'
    # and, for cleanliness, will not be passed as args to dynamodb_query
    if 'geom' in args.data:
        args.data.pop('geom')
    if 'node_id' in args.data:
        args.data.pop('node_id')

    data = dynamodb_query.query(args.data)

    resp = json_response_base(data)
    resp = make_response(json.dumps(resp), 200)
    resp.headers['Content-Type'] = 'application/json'

    return resp
