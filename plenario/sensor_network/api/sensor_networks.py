import json
import sqlalchemy
import threading

from flask import request, make_response
from shapely import wkb
from sqlalchemy.exc import NoSuchTableError

from plenario.api.common import cache, crossdomain, CACHE_TIMEOUT
from plenario.api.common import make_cache_key
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
def get_sensors(network_name=None, feature=None, sensor=None, node_id=None):
    fields = ('network_name', 'feature', 'sensor', 'node_id')

    args = {}
    if network_name:
        args['network_name'] = network_name
    if feature:
        args['feature'] = feature
    if sensor:
        args['sensor'] = sensor
    if node_id:
        args['node_id'] = node_id

    validator = SensorNetworkValidator(only=fields)
    validated_args = validate(validator, args)
    if validated_args.errors:
        return bad_request(validated_args.errors)

    return _get_sensors(validated_args)


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def get_observations(network_name=None):
    fields = ('network_name', 'nodes',
              'start_datetime', 'end_datetime',
              'location_geom__within',
              'features_of_interest', 'sensors',
              'limit', 'offset'
              )

    args = request.args.to_dict()

    if network_name is None:
        return bad_request("Must specify a network name")
    args['network_name'] = network_name

    if 'nodes' in args:
        args['nodes'] = args['nodes'].split(',')

    if 'features_of_interest' in args:
        args['features_of_interest'] = args['features_of_interest'].split(',')

    if 'sensors' in args:
        args['sensors'] = args['sensors'].split(',')

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
        q = q.filter(NodeMeta.sensor_network == network_name)

    # if the user specified a node ID list, filter to those nodes
    if nodes:
        q = q.filter(NodeMeta.id.in_(nodes))

    # If the user specified a geom, filter results to those within its shape
    if geom:
        q = q.filter(NodeMeta.location.ST_Within(
            sqlalchemy.func.ST_GeomFromGeoJSON(geom)
        ))

    return q


def observation_query(args, table):
    params = ('nodes',
              'start_datetime', 'end_datetime',
              'sensors')

    vals = (args.data.get(k) for k in params)
    nodes, start_datetime, end_datetime, sensors = vals

    q = redshift_session.query(table)

    q = q.filter(table.c.datetime.between(start_datetime, end_datetime))
    q = q.filter(table.c.node_id.in_(nodes))

    if sensors:
        q = q.filter(table.c.sensor.in_(sensors))

    return q


def format_network_metadata(network):
    network_response = {
        'name': network.name,
        'features_of_interest': FeatureOfInterest.index(network.name),
        'nodes': NodeMeta.index(network.name),
        'sensors': Sensor.index(network.name),
        'info': network.info
    }

    return network_response


def format_node_metadata(node):
    node_response = {
        "type": "Feature",
        'geometry': {
            "type": "Point",
            "coordinates": [wkb.loads(bytes(node.location.data)).y, wkb.loads(bytes(node.location.data)).x],
        },
        "properties": {
            "id": node.id,
            "network_name": node.sensor_network,
            "sensors": [sensor.name for sensor in node.sensors],
            "info": node.info,
            "features_of_interest": None,
        },
    }

    features = []
    for sensor in node.sensors:
        for prop in sensor.observed_properties.itervalues():
            features.append(prop.split('.')[0])
    node_response['properties']['features_of_interest'] = features

    return node_response


def format_feature(feature):
    feature_response = {
        'name': feature.name,
        'observed_properties': feature.observed_properties,
    }

    return feature_response


def format_sensor(sensor):
    sensor_response = {
        'name': sensor.name,
        'observed_properties': sensor.observed_properties.values(),
        'info': sensor.info
    }

    return sensor_response


def format_observation(obs, table):
    obs_response = {
        'node_id': obs.node_id,
        'meta_id': obs.meta_id,
        'datetime': obs.datetime.isoformat().split('+')[0],
        'sensor': obs.sensor,
        'feature_of_interest': table.name,
        'results': {}
    }
    for prop in (set([c.name for c in table.c]) - {'node_id', 'datetime', 'sensor', 'meta_id'}):
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
            if (feature.name in FeatureOfInterest.index(args.data['network_name']) or
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
            if (sensor.name in Sensor.index(args.data['network_name']) or
                args.data['network_name'] is None) and
            (args.data['feature'] in [prop.split('.')[0] for prop in sensor.observed_properties.itervalues()] or
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
    print nodes_to_query

    args.data['nodes'] = nodes_to_query

    features = args.data['features_of_interest']
    print features

    # if the user specified a sensor list,
    # only query feature tables that those sensors report on
    if 'sensors' in request.args:

        sensors = [sensor for sensor in session.query(Sensor).filter(Sensor.name.in_(args.data['sensors']))]
        all_features = []
        for sensor in sensors:
            for foi in list(set([prop.split('.')[0] for prop in sensor.observed_properties.itervalues()])):
                all_features.append(foi)
        features = set(features).intersection(all_features)

    tables = []
    meta = sqlalchemy.MetaData()
    for feature in features:
        table_name = feature.lower()
        try:
            tables.append(sqlalchemy.Table(table_name, meta, autoload=True, autoload_with=redshift_engine))
        except (AttributeError, NoSuchTableError):
            return bad_request("Table {} not found".format(table_name))

    # TODO: make limit on threaded query reliably return the correct number of results
    data = []
    threads = []
    for table in tables:
        t = threading.Thread(target=_thread_query, args=(table, len(tables), data, args))
        threads.append(t)
        t.start()

    # Now wait for each thread to terminate
    for thread in threads:
        thread.join()

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
    q = observation_query(args, table)
    q = q.limit(args.data['limit'] / num_tables)
    q = q.offset(args.data['offset'] / num_tables) if args.data['offset'] else q
    for obs in q.all():
        data.append(format_observation(obs, table))
