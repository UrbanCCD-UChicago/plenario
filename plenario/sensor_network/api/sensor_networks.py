import json
import threading

from dateutil.parser import parse
from flask import request, make_response
from shapely import wkb
from sqlalchemy import MetaData, Table, func as sqla_fn
from sqlalchemy.exc import NoSuchTableError

from plenario.api.common import cache, crossdomain, CACHE_TIMEOUT
from plenario.api.common import make_cache_key, unknown_object_json_handler
from plenario.sensor_network.api.sensor_response import json_response_base, bad_request
from plenario.sensor_network.api.sensor_validator import Validator, validate, NodeAggregateValidator
from plenario.database import session, redshift_session, redshift_engine
from plenario.sensor_network.sensor_models import NetworkMeta, NodeMeta, FeatureOfInterest, Sensor

from sensor_aggregate_functions import aggregate_fn_map


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def get_network_metadata(network_name=None):
    """Return metadata for some network. If no network_name is specified, the
    default is to return metadata for all sensor networks.

    :endpoint: /sensor-networks/<network-name>
    :param network_name: (str) network name
    :returns: (json) response"""

    fields = ('network_name',)

    args = {"network_name": network_name.lower() if network_name else None}

    validator = Validator(only=fields)
    validated_args = validate(validator, args)
    if validated_args.errors:
        return bad_request(validated_args.errors)

    return _get_network_metadata(validated_args)


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def get_node_metadata(network_name, node_id=None):
    """Return metadata about nodes for some network. If no node_id or
    location_geom__within is specified, the default is to return metadata
    for all nodes within the network.

    :endpoint: /sensor-networks/<network-name>/nodes/<node-id>
    :param network_name: (str) network that exists in sensor__network_metadata
    :param node_id: (str) node that exists in sensor__node_metadata
    :returns: (json) response"""

    fields = ('network_name', 'node_id', 'nodes', 'location_geom__within')

    args = request.args.to_dict()
    args["network_name"] = network_name.lower()
    args["node_id"] = node_id.lower() if node_id else None

    validator = Validator(only=fields)
    validated_args = validate(validator, args)
    if validated_args.errors:
        return bad_request(validated_args.errors)

    return _get_node_metadata(validated_args)


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def get_features(network_name, feature=None):
    """Return metadata about features for some network. If no feature is
    specified, return metadata about all features within the network.

    :endpoint: /sensor-networks/<network_name>/features_of_interest/<feature>
    :param network_name: (str) network name from sensor__network_metadata
    :param feature: (str) name from sensor__features_of_interest
    :returns: (json) response"""

    fields = ('network_name', 'feature')

    args = dict()
    args['network_name'] = network_name.lower()
    args['feature'] = feature.lower() if feature else None

    validator = Validator(only=fields)
    validated_args = validate(validator, args)
    if validated_args.errors:
        return bad_request(validated_args.errors)

    return _get_features(validated_args)


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def get_sensors(network_name, feature=None, sensor=None, node_id=None):
    """Return metadata for all sensors within a network. Sensors can also be
    be filtered by various other properties. If no single sensor is specified,
    the default is to return metadata for all sensors within the network.

    :endpoint: /sensor-networks/<network_name>/sensors/<sensor>
    :param network_name: (str) name from sensor__network_metadata
    :param feature: (str) name from sensor__features_of_interest
    :param sensor: (str) name from sensor__sensors
    :param node_id: (str) name from sensor__node_metadata
    :returns: (json) response"""

    fields = ('network_name', 'feature', 'sensor', 'node_id')

    args = dict()
    args["network_name"] = network_name.lower()
    args["feature"] = feature.lower() if feature else None
    args["sensor"] = sensor.lower() if sensor else None
    args["node_id"] = node_id.lower() if node_id else None

    validator = Validator(only=fields)
    validated_args = validate(validator, args)
    if validated_args.errors:
        return bad_request(validated_args.errors)

    return _get_sensors(validated_args)


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def get_observations(network_name=None):
    fields = ('network_name', 'nodes', 'start_datetime', 'end_datetime',
              'location_geom__within', 'features_of_interest', 'sensors',
              'limit', 'offset')

    args = request.args.to_dict()

    if network_name is None:
        return bad_request("Must specify a network name")
    args['network_name'] = network_name

    if 'nodes' in args:
        args['nodes'] = args['nodes'].split(',')
        args["nodes"] = (n.lower() for n in args["nodes"])

    if 'features_of_interest' in args:
        args['features_of_interest'] = args['features_of_interest'].split(',')
        args["features_of_interest"] = (f.lower() for f in args["features_of_interest"])

    if 'sensors' in args:
        args['sensors'] = args['sensors'].split(',')
        args["sensors"] = (s.lower() for s in args["sensors"])

    validator = Validator(only=fields)
    validated_args = validate(validator, args)
    if validated_args.errors:
        return bad_request(validated_args.errors)

    return _get_observations(validated_args)


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def get_node_aggregations(network_name):
    """Aggregate individual node observations up to larger units of time.
    Do so by applying aggregate functions on all observations found within
    a specified window of time.

    :endpoint: /sensor-networks/<network-name>/aggregate
    :param network_name: (str) from sensor__network_metadata
    :returns: (json) response"""

    fields = ("network_name", "node_id", "function", "feature", "start_datetime")

    args = request.args.to_dict()
    args["network_name"] = network_name

    validated_args = validate(NodeAggregateValidator(only=fields), args)
    if validated_args.errors:
        return bad_request(validated_args.errors)

    validated_args.data["node_id"] = validated_args.data["node_id"].lower()
    validated_args.data["function"] = validated_args.data["function"].lower()
    validated_args.data["feature"] = validated_args.data["feature"].lower()

    result = _get_node_aggregations(validated_args)
    return node_aggregations_response(validated_args, result)


def _get_node_aggregations(args):

    return aggregate_fn_map[args.data.get("function")](args)


def node_aggregations_response(args, result):

    resp = json_response_base(args, result, args.data)
    resp = make_response(json.dumps(resp, default=unknown_object_json_handler), 200)
    resp.headers['Content-Type'] = 'application/json'

    return resp


def node_metadata_query(args):
    """Create a SQLAlchemy query for querying node metadata. Used in the
    _get_node_metadata route logic method.

    :param args: (ValidatorResult) with args held in the data property
    :returns: (sqlalchemy.orm.query.Query) object"""

    params = ('network_name', 'node_id', 'nodes', 'geom')
    network_name, node_id, nodes, geom = (args.data.get(k) for k in params)

    geom_filter = NodeMeta.location.ST_Within(sqla_fn.ST_GeomFromGeoJSON(geom))

    query = session.query(NodeMeta)
    query = query.filter(sqla_fn.lower(NodeMeta.sensor_network) == sqla_fn.lower(network_name))
    query = query.filter(sqla_fn.lower(NodeMeta.id) == sqla_fn.lower(node_id)) if node_id else query
    query = query.filter(sqla_fn.lower(NodeMeta.id).in_(nodes)) if nodes else query
    query = query.filter(geom_filter) if geom else query

    return query


def observation_query(args, table):
    params = ('nodes', 'start_datetime', 'end_datetime', 'sensors')
    nodes, start_datetime, end_datetime, sensors = (args.data.get(k) for k in params)

    q = redshift_session.query(table)
    q = q.filter(table.c.datetime.between(start_datetime, end_datetime))
    q = q.filter(sqla_fn.lower(table.c.node_id).in_(nodes)) if nodes else q
    q = q.filter(sqla_fn.lower(table.c.sensor).in_(sensors)) if sensors else q

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
        },
    }

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
    network_name = args.data.get("network_name")

    query = session.query(NetworkMeta)
    query = query.filter(sqla_fn.lower(NetworkMeta.name) == sqla_fn.lower(network_name)) if network_name else query
    data = [format_network_metadata(network) for network in query.all()]

    # Remove null query keys
    null_keys = [k for k in args.data if args.data[k] is None]
    for key in null_keys:
        del args.data[key]

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

    target_feature = args.data.get("feature")
    features = session.query(FeatureOfInterest).all()
    features_index = FeatureOfInterest.index(args.data.get("network_name"))

    data = [format_feature(feature) for feature in features
            if feature.name.lower() in features_index and
            (target_feature is None or feature.name.lower() == target_feature.lower())]

    # don't display null query arguments
    null_args = [field for field in args.data if args.data[field] is None]
    for null_arg in null_args:
        args.data.pop(null_arg)

    resp = json_response_base(args, data, args.data)
    resp = make_response(json.dumps(resp), 200)
    resp.headers['Content-Type'] = 'application/json'

    return resp


def _get_sensors(args):
    feature, sensor_name, network = (args.data.get(k) for k in ("feature", "sensor", "network_name"))

    valid_sensors = [s for s in Sensor.index(network) if sensor_name is None or s == sensor_name.lower()]
    sensors = session.query(Sensor).all()

    data = []
    for sensor in sensors:
        valid_properties = [p.split(".")[0] for p in sensor.observed_properties.values()]
        if sensor.name.lower() not in valid_sensors:
            continue
        if feature is not None and feature not in valid_properties:
            continue
        data.append(format_sensor(sensor))

    # don't display null query arguments
    null_args = [field for field in args.data if args.data[field] is None]
    for null_arg in null_args:
        args.data.pop(null_arg)

    resp = json_response_base(args, data, args.data)
    resp = make_response(json.dumps(resp), 200)
    resp.headers['Content-Type'] = 'application/json'

    return resp


def _get_observations(args):
    nodes_to_query = [node.id.lower() for node in node_metadata_query(args).all()]
    args.data['nodes'] = nodes_to_query

    features = args.data['features_of_interest']
    target_sensors = args.data.get("sensors")

    # if the user specified a sensor list,
    # only query feature tables that those sensors report on
    if target_sensors:
        sensors = session.query(Sensor)
        sensors = sensors.filter(sqla_fn.lower(Sensor.name).in_(target_sensors))
        sensors = sensors.all()
        all_features = []
        for sensor in sensors:
            for foi in set([prop.split('.')[0].lower() for prop in sensor.observed_properties.itervalues()]):
                all_features.append(foi)
        features = set(features).intersection(all_features)

    tables = []
    meta = MetaData()
    for feature in features:
        table_name = feature.lower()
        try:
            tables.append(Table(table_name, meta, autoload=True, autoload_with=redshift_engine))
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

    # Sort the data by datetime
    data.sort(key=lambda x: parse(x["datetime"]))

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
