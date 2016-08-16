import json
import shapely.geometry
import sqlalchemy
import ast
import threading
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
from plenario.sensor_network.api.sensor_condition_builder import parse_tree
from plenario.sensor_network.api.sensor_response import json_response_base, bad_request
from plenario.sensor_network.api.sensor_validator import SensorNetworkValidator, validate
from plenario.database import session, redshift_session, redshift_engine
from plenario.sensor_network.sensor_models import NetworkMeta, NodeMeta, FeatureOfInterest, Sensor


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def get_network_metadata(network_name=None):
    fields = ('network_name',)

    args = {}
    if network_name:
        args['network_name'] = network_name
    validator = SensorNetworkValidator(only=fields)
    validated_args = validate(validator, args)
    if validated_args.errors:
        return bad_request(validated_args.errors)

    return _get_network_metadata(validated_args)


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def get_node_metadata(node_id=None, network_name=None):
    fields = ('network_name', 'node_id', 'nodes',
              'location_geom__within')

    args = request.args.to_dict()
    if network_name:
        args['network_name'] = network_name
    if node_id:
        args['node_id'] = node_id
    if 'nodes' in args:
        try:
            args['nodes'] = ast.literal_eval(args['nodes'])
        except (SyntaxError, ValueError) as e:
            return bad_request("Cannot parse 'nodes' filter. Causes error {}".format(e))

    # do we want to allow these?
    if 'nodes' in args and 'node_id' in args:
        return bad_request("Cannot specify single node ID and nodes filter")
    if 'location_geom__within' in args and 'node_id' in args:
        return bad_request("Cannot specify single node ID and geom filter")

    validator = SensorNetworkValidator(only=fields)
    validated_args = validate(validator, args)
    if validated_args.errors:
        return bad_request(validated_args.errors)

    return _get_node_metadata(validated_args)


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def get_features(network_name=None, feature=None):
    fields = ('network_name', 'feature')

    args = {}
    if network_name:
        args['network_name'] = network_name
    if feature:
        args['feature'] = feature

    validator = SensorNetworkValidator(only=fields)
    validated_args = validate(validator, args)
    if validated_args.errors:
        return bad_request(validated_args.errors)

    return _get_features(validated_args)


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def get_sensors(network_name=None, feature=None, sensor=None):
    fields = ('network_name', 'feature', 'sensor')

    args = {}
    if network_name:
        args['network_name'] = network_name
    if feature:
        args['feature'] = feature
    if sensor:
        args['sensor'] = sensor

    validator = SensorNetworkValidator(only=fields)
    validated_args = validate(validator, args)
    if validated_args.errors:
        return bad_request(validated_args.errors)

    return _get_sensors(validated_args)


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def get_observations(network_name=None, node_id=None):
    fields = ('network_name', 'node_id', 'nodes',
              'start_datetime', 'end_datetime',
              'location_geom__within', 'filter',
              'features_of_interest', 'sensors',
              'limit', 'offset'
              )

    args = request.args.to_dict()

    if network_name is None:
        return bad_request("Must specify network name")
    args['network_name'] = network_name

    if node_id:
        args['node_id'] = node_id

    if 'nodes' in args:
        args['nodes'] = args['nodes'].split(',')

    if 'features_of_interest' in args:
        args['features_of_interest'] = args['features_of_interest'].split(',')

    if 'sensors' in args:
        args['sensors'] = args['sensors'].split(',')

    # do we want to allow these?
    if 'nodes' in args and 'node_id' in args:
        return bad_request("Cannot specify single node ID and nodes filter")
    if 'location_geom__within' in args and 'node_id' in args:
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
              'sensors',
              'table')

    vals = (args.data.get(k) for k in params)
    nodes, filter, start_datetime, end_datetime, sensors, table = vals

    q = redshift_session.query(table)

    q = q.filter(table.c.datetime.between(start_datetime, end_datetime))
    q = q.filter(table.c.nodeid.in_(nodes))

    if sensors:
        q = q.filter(table.c.sensor.in_(sensors))

    # if filter:
    #    q = q.filter(parse_tree(table, filter))

    return q


def format_network_metadata(network):
    network_response = {
        'name': network.name,
        'features_of_interest': [feature.name for feature in network.featuresOfInterest],
        'nodes': [node.id for node in network.nodes],
        'info': network.info
    }

    return network_response


def format_node_metadata(node):
    node_response = {
        'id': node.id,
        'network_name': node.sensorNetwork,
        'location': {
            'lat': wkb.loads(bytes(node.location.data)).y,
            'lon': wkb.loads(bytes(node.location.data)).x
        },
        'info': node.info
    }

    return node_response


def format_feature(feature):
    feature_response = {
        'name': feature.name,
        'observed_properties': feature.observedProperties['observedProperties'],
    }

    return feature_response


def format_sensor(sensor):
    sensor_response = {
        'name': sensor.name,
        'features_of_interest': [feature.name for feature in sensor.featuresOfInterest],
        'properties': sensor.properties,
        'info': sensor.info
    }

    return sensor_response


def format_observation(obs, table):
    obs_response = {
        'node_id': obs.nodeid,
        'datetime': obs.datetime.isoformat().split('+')[0],
        'sensor': obs.sensor,
        'feature_of_interest': table.name,
        'results': {}
    }
    for prop in (set([c.name for c in table.c]) - {'nodeid', 'datetime', 'sensor', 'procedures'}):
        obs_response['results'][prop] = getattr(obs, prop)

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
            if (args.data['network_name'] in [network.name for network in feature.sensorNetworks] or
                args.data['network_name'] is None) and
            (feature.name == args.data['feature'] or args.data['feature'] is None)]

    # don't display null query arguments
    null_args = [field for field in args.data if args.data[field] is None]
    for null_arg in null_args:
        args.data.pop(null_arg)

    resp = json_response_base(args, data, args.data)
    resp = make_response(json.dumps(resp), 200)
    resp.headers['Content-Type'] = 'application/json'

    return resp


def _get_sensors(args):
    q = session.query(Sensor)
    data = [format_sensor(sensor) for sensor in q.all()
            if (args.data['network_name'] in [network.name for network in sensor.sensorNetworks] or
                args.data['network_name'] is None) and
            (args.data['feature'] in [feature.name for feature in sensor.featuresOfInterest] or
             args.data['feature'] is None) and
            (sensor.name == args.data['sensor'] or args.data['sensor'] is None)]

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

    features = args.data['features_of_interest']

    # if the user specified a sensor list,
    # only query feature tables that those sensors report on
    try:
        # Will throw KeyError if no 'sensor' argument was given
        s = request.args['sensors']

        sensors = [sensor for sensor in session.query(Sensor).filter(Sensor.name.in_(args.data['sensors']))]
        all_features = []
        for sensor in sensors:
            for foi in sensor.featuresOfInterest:
                all_features.append(foi.name)
        features = set(features).intersection(all_features)
    except KeyError:
        pass

    tables = []
    meta = sqlalchemy.MetaData()
    for feature in features:
        table_name = feature.lower()
        try:
            tables.append(sqlalchemy.Table(table_name, meta, autoload=True, autoload_with=redshift_engine))
        except (AttributeError, NoSuchTableError):
            return bad_request("Table {} not found".format(table_name))

    data = []
    threads = []
    for table in tables:
        t = threading.Thread(target=_thread_query, args=(table, len(tables), data, args))
        threads.append(t)
        t.start()
        t.join()

    # if the user didn't specify a 'nodes' filter, don't display nodes in the query output
    if 'nodes' not in request.args:
        args.data.pop('nodes')

    # if the user didn't specify a 'features_of_interest' filter, don't display features in the query output
    if 'features_of_interest' not in request.args:
        args.data.pop('features_of_interest')

    # if the user didn't specify a 'sensors' filter, don't display sensors in the query output
    if 'sensors' not in request.args:
        args.data.pop('sensors')

    # 'geom' is encapsulated within 'nodes'
    # and will not be displayed in the query output
    if 'geom' in args.data:
        args.data.pop('geom')

    # the reflected table object used for querying should not be returned in the output
    if 'table' in args.data:
        args.data.pop('table')

    # get rid of those pesky +00:00 timezones
    if 'start_datetime' in request.args:
        args.data['start_datetime'] = args.data['start_datetime'].split("+")[0]
    if 'end_datetime' in request.args:
        args.data['end_datetime'] = args.data['end_datetime'].split("+")[0]

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


def _thread_query(table, num_tables, data, args):
    args.data['table'] = table
    q = observation_query(args)
    q = q.limit(args.data['limit'] / num_tables)
    q = q.offset(args.data['offset'] / num_tables) if args.data['offset'] else q
    for obs in q.all():
        data.append(format_observation(obs, table))
