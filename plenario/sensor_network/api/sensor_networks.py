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
from sqlalchemy.exc import NoSuchTableError

from plenario.api.common import cache, crossdomain, CACHE_TIMEOUT
from plenario.api.common import make_cache_key
from plenario.api.condition_builder import parse_tree
from plenario.sensor_network.api.sensor_response import json_response_base, bad_request
from plenario.sensor_network.api.sensor_validator import SensorNetworkValidator, validate
from plenario.database import session, redshift_session, redshift_engine
from plenario.sensor_network.sensor_models import NetworkMeta, NodeMeta, FeatureOfInterest


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def get_network_metadata(network_name=None):
    fields = ('network_name',)

    args = {'network_name': network_name}
    validator = SensorNetworkValidator(only=fields)
    validated_args = validate(validator, args)
    if validated_args.errors:
        return bad_request(validated_args.errors)

    return _get_network_metadata(validated_args)


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def get_node_metadata(node_id=None, network_name=None):
    fields = ('network_name', 'node_id', 'nodes',
              'location_geom__within',)

    args = request.args.to_dict()
    if network_name:
        args['network_name'] = network_name
    if node_id:
        args['node_id'] = node_id
    if 'nodes' in args.keys():
        try:
            args['nodes'] = ast.literal_eval(args['nodes'])
        except (SyntaxError, ValueError) as e:
            return bad_request("Cannot parse 'nodes' filter. Causes error {}".format(e))

    validator = SensorNetworkValidator(only=fields)
    validated_args = validate(validator, args)
    if validated_args.errors:
        return bad_request(validated_args.errors)

    return _get_node_metadata(validated_args)


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def get_features(network_name=None, feature=None):
    fields = ('network_name', 'feature')

    args = {'network_name': network_name}
    if feature:
        args['feature'] = feature

    validator = SensorNetworkValidator(only=fields)
    validated_args = validate(validator, args)
    if validated_args.errors:
        return bad_request(validated_args.errors)

    return _get_features(validated_args)


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
    if 'nodes' in args.keys():
        try:
            args['nodes'] = ast.literal_eval(args['nodes'])
        except (SyntaxError, ValueError) as e:
            return bad_request("Cannot parse 'nodes' filter. Causes error {}".format(e))

    # do we want to allow these?
    if 'nodes' in args.keys() and 'node_id' in args.keys():
        return bad_request("Cannot specify single node ID and nodes filter")
    if 'location_geom__within' in args.keys() and 'node_id' in args.keys():
        return bad_request("Cannot specify single node ID and geom filter")

    validator = SensorNetworkValidator(only=fields)
    validated_args = validate(validator, args)
    if validated_args.errors:
        return bad_request(validated_args.errors)

    return _get_observations(validated_args)


def node_metadata_query(args):
    params = ('network_name', 'node_id', 'nodes',
              'geom')
    vals = (args.data.get(k) for k in params)
    network_name, node_id, nodes, geom = vals

    q = session.query(NodeMeta)

    # if path specified a sensor network or node ID, filter results accordingly
    if node_id:
        q = q.filter(NodeMeta.id == node_id)
    if network_name:
        q = q.filter(NodeMeta.sensorNetwork == network_name)

    # if the user specified a node ID list, filter to those nodes
    if nodes:
        q = q.filter(NodeMeta.id.in_(nodes))

    # If the user specified a geom, filter results to those within its shape
    if geom:
        q = q.filter(NodeMeta.location.ST_Within(
            sqlalchemy.func.ST_GeomFromGeoJSON(geom)
        ))

    return q


def observation_query(args):
    params = ('nodes', 'filter',
              'start_datetime', 'end_datetime',
              'table')

    vals = (args.data.get(k) for k in params)
    nodes, filter, start_datetime, end_datetime, table = vals

    q = redshift_session.query(table)

    q = q.filter(table.c.datetime.between(start_datetime, end_datetime))
    q = q.filter(table.c.nodeid.in_(nodes))
    if filter:
        q = q.filter(parse_tree(table, filter))

    return q


def format_network_metadata(network):
    network_response = {
        'name': network.name,
        'nodeMetadata': network.nodeMetadata,
        'featuresOfInterest': [feature.name for feature in network.featuresOfInterest],
        'nodes': [node.id for node in network.nodes]
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
        'featuresOfInterest': [foi.name for foi in node.featuresOfInterest],
        'procedures': node.procedures
    }

    return node_response


def format_feature(feature):
    feature_response = {
        'name': feature.name,
        'sensorNetwork': feature.sensorNetwork,
        'observedProperties': feature.observedProperties['observedProperties']
    }

    return feature_response


def format_observation(obs, feature_properties):
    obs_response = {
        'node_id': obs.nodeid,
        'datetime': obs.datetime.isoformat(),
        'featureOfInterest': obs.feature,
        'sensor': obs.sensor,
        'results': {
            feature_properties[obs.feature][0]: obs.property1
        }
    }

    if len(feature_properties[obs.feature]) > 1:
        obs_response['results'][feature_properties[obs.feature][1]] = obs.property2
    if len(feature_properties[obs.feature]) > 2:
        obs_response['results'][feature_properties[obs.feature][2]] = obs.property3
    if len(feature_properties[obs.feature]) > 3:
        obs_response['results'][feature_properties[obs.feature][3]] = obs.property4

    return obs_response


def _get_network_metadata(args):
    q = session.query(NetworkMeta)
    data = [format_network_metadata(network) for network in q.all()
            if network.name == args.data['network_name'] or args.data['network_name'] is None]

    # don't display null query arguments
    null_args = [field for field in args.data if args.data[field] is None]
    for null_arg in null_args:
        args.data.pop(null_arg)

    resp = json_response_base(args, data, args.data)
    resp = make_response(json.dumps(resp), 200)
    resp.headers['Content-Type'] = 'application/json'

    return resp


def _get_node_metadata(args):
    q = node_metadata_query(args)
    data = [format_node_metadata(node) for node in q.all()]

    # don't display null query arguments
    null_args = [field for field in args.data if args.data[field] is None]
    for null_arg in null_args:
        args.data.pop(null_arg)
    # if the user didn't specify a 'nodes' filter, don't display nodes in the query output
    if 'nodes' not in request.args:
        args.data.pop('nodes')

    resp = json_response_base(args, data, args.data)
    resp = make_response(json.dumps(resp), 200)
    resp.headers['Content-Type'] = 'application/json'

    return resp


def _get_features(args):
    q = session.query(FeatureOfInterest)
    data = [format_feature(feature) for feature in q.all()
            if (feature.sensorNetwork == args.data['network_name'] or args.data['network_name'] is None) and
            (feature.name == args.data['feature'] or args.data['feature'] is None)]

    # don't display null query arguments
    null_args = [field for field in args.data if args.data[field] is None]
    for null_arg in null_args:
        args.data.pop(null_arg)

    resp = json_response_base(args, data, args.data)
    resp = make_response(json.dumps(resp), 200)
    resp.headers['Content-Type'] = 'application/json'

    return resp


def _get_observations(args):
    nodes_to_query = [node.id for node in node_metadata_query(args).all()]

    args.data['nodes'] = nodes_to_query

    try:
        meta = sqlalchemy.MetaData()
        table_name = args.data['network_name'].lower()
        table = sqlalchemy.Table(table_name, meta, autoload=True, autoload_with=redshift_engine)
    except (AttributeError, NoSuchTableError):
        msg = "Table name {} not found in Redshift".format(table_name)
        return bad_request(msg)

    args.data['table'] = table

    # query observations
    q = observation_query(args)

    # determine the features returned in the query
    all_features = set([obs.feature for obs in q.all()])

    # create a dictionary containing all necessary features and their properties in order to format response
    feature_properties = {}
    for feature in all_features:
        fq = session.query(FeatureOfInterest)
        fq = fq.filter(FeatureOfInterest.name == feature)
        properties_list = fq.first().observedProperties['observedProperties']
        feature_properties[feature] = [properties_list[i]['name'] for i in range(0, len(properties_list))]

    data = [format_observation(obs, feature_properties) for obs in q.all()]

    # if the user didn't specify a 'nodes' filter, don't display nodes in the query output
    if 'nodes' not in request.args:
        args.data.pop('nodes')

    # 'geom' is encapsulated within 'nodes'
    # and will not be displayed in the query output
    if 'geom' in args.data:
        args.data.pop('geom')

    # the reflected table object used for querying should returned in the output
    if 'table' in args.data:
        args.data.pop('table')

    # don't display null query arguments
    null_args = [field for field in args.data if args.data[field] is None]
    for null_arg in null_args:
        args.data.pop(null_arg)

    # combine path and request arguments
    display_args = {}
    display_args.update(request.args)
    display_args.update(args.data)

    resp = json_response_base(args, data, display_args)
    resp = make_response(json.dumps(resp), 200)
    resp.headers['Content-Type'] = 'application/json'

    return resp
