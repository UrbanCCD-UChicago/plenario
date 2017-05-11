import csv
import io
import json
from datetime import datetime, timedelta

import boto3
from flask import request, Response, stream_with_context, jsonify, redirect
from marshmallow import Schema
from marshmallow.exceptions import ValidationError
from marshmallow.fields import Field, List, DateTime, Integer, String, Float
from marshmallow.validate import Range
from shapely import wkb
from sqlalchemy import MetaData, func as sqla_fn, and_, asc, desc
from sqlalchemy.orm.exc import NoResultFound

from plenario.api.common import cache, crossdomain, make_fragment_str
from plenario.api.common import extract_first_geometry_fragment
from plenario.api.common import make_cache_key, unknown_object_json_handler
from plenario.api.condition_builder import parse_tree
from plenario.api.validator import valid_tree
from plenario.database import redshift_session, redshift_engine, redshift_base
from plenario.settings import S3_BUCKET
from plenario.utils.helpers import reflect


# Cache timeout of 5 minutes
CACHE_TIMEOUT = 60 * 10


class Network(Field):

    def _deserialize(self, value, attr, data):
        try:
            value = value.lower()
            query = NetworkMeta.query.filter(NetworkMeta.name == value)
            return query.one()
        except NoResultFound:
            raise ValidationError("{} does not exist".format(value))

    def _serialize(self, value, attr, obj):
        return value.name.lower()


class Node(Field):

    def _deserialize(self, value, attr, data):
        try:
            value = value.lower()
            query = NodeMeta.query.filter(NodeMeta.id == value)
            return query.one()
        except NoResultFound:
            raise ValidationError("{} does not exist".format(value))

    def _serialize(self, value, attr, obj):
        return value.id.lower()


class Sensor(Field):

    def _deserialize(self, value, attr, data):
        try:
            value = value.lower()
            query = SensorMeta.query.filter(SensorMeta.name == value)
            return query.one()
        except NoResultFound:
            raise ValidationError("{} does not exist".format(value))

    def _serialize(self, value, attr, obj):
        return value.name.lower()


class Feature(Field):

    def _deserialize(self, value, attr, data):
        try:
            value = value.lower().split('.', 1)[0]
            query = FeatureMeta.query.filter(FeatureMeta.name == value)
            return query.one()
        except NoResultFound:
            raise ValidationError("{} does not exist".format(value))

    def _serialize(self, value, attr, obj):
        return value.name.lower()


class Geom(Field):

    def _deserialize(self, value, attr, data):
        try:
            return make_fragment_str(extract_first_geometry_fragment(value))
        except (ValueError, AttributeError):
            raise ValidationError("Invalid geom: {}".format(value))


class ConditionTree(Field):

    def _deserialize(self, value, attr, data):
        feature = request.args['feature']
        network = request.view_args['network']

        try:
            parsed_json = json.loads(value)
            table = redshift_base.metadata.tables[network + '__' + feature]
            valid_tree(table, parsed_json)
            return parse_tree(table, parsed_json)
        except (KeyError) as err:
            raise ValidationError(str(err))


class Validator(Schema):

    feature = Feature()
    features = List(Feature(allow_none=True))
    network = Network()
    networks = List(Network(allow_none=True))
    node = Node()
    nodes = List(Node(allow_none=True))
    sensor = Sensor()
    sensors = List(Sensor(allow_none=True))

    datetime = DateTime()
    start_datetime = DateTime()
    end_datetime = DateTime()

    agg = String(missing="hour")
    filter = ConditionTree(allow_none=True, missing=None, default=None)
    function = String(missing="avg")
    limit = Integer(missing=1000)
    offset = Integer(missing=0, validate=Range(0))
    geom = Geom()


class NearestValidator(Validator):

    feature = Feature(required=True)
    lat = Float(required=True)
    lng = Float(required=True)


class NoLimitValidator(Validator):

    data_type = String(allow_none=True)
    limit = Integer(allow_none=True)


class AggregateValidator(Validator):

    node = Node(required=True)
    feature = Feature(required=True)


@crossdomain(origin="*")
def get_network_map(network: str) -> Response:
    """Map of network and the relationships of the elements it contains."""

    try:
        network = NetworkMeta.query.get(network)
    except NoResultFound:
        bad_request("Invalid network name: %s" % network)
    return jsonify(network.tree())


# @cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def get_network_metadata(network: str = None) -> Response:
    """Return metadata for some network. If no network_name is specified, the
    default is to return metadata for all sensor networks.

    :endpoint: /sensor-networks/<network-name>"""

    args = request.args.to_dict()
    args.update({"networks": [network]})

    validator = Validator()
    validated = validator.load(args)
    if validated.errors:
        return bad_request(validated.errors)

    networks = validated.data["networks"]
    if network is None:
        networks = NetworkMeta.query.all()

    result = [format_network_metadata(n) for n in networks]
    return jsonify(json_response_base(validated, result, args))


# @cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def get_node_metadata(network: str, node: str = None) -> Response:
    """Return metadata about nodes for some network. If no node_id or
    location_geom__within is specified, the default is to return metadata
    for all nodes within the network.

    :endpoint: /sensor-networks/<network-name>/nodes/<node>"""

    args = request.args.to_dict()
    args.update({"network": network, "nodes": [node]})

    validator = Validator()
    validated = validator.load(args)
    if validated.errors:
        return bad_request(validated.errors)

    network = validated.data["network"]
    nodes = validated.data["nodes"]

    if node is None:
        nodes = NodeMeta.all(network.name)
    elif node.lower() not in network.tree():
        return bad_request("Invalid node {} for {}".format(node, network))

    geojson = validated.data.get('geom')
    if geojson:
        nodes_within_geom = NodeMeta.within_geojson(network, geojson).all()
        if not nodes_within_geom:
            return bad_request("No features found within {}!".format(geojson))
        nodes = nodes_within_geom

    result = [format_node_metadata(n) for n in nodes]
    return jsonify(json_response_base(validated, result, args))


@crossdomain(origin="*")
def get_node_download(network: str, node: str):
    """Return an aws presigned-url to download a month of tar'd node data."""

    args = request.args.to_dict()
    args.update({
        "network": network,
        "node": node,
        "datetime": args.get('datetime')
    })

    validator = Validator()
    validated = validator.load(args)
    if validated.errors:
        return bad_request(validated.errors)

    dt = validated.data['datetime']
    year_and_month = '{}-{}'.format(dt.year, dt.month)
    key = year_and_month + '/' + node + '.tar.gz'
    return redirect(presigned_url(S3_BUCKET, key, node))


def presigned_url(bucket: str, key: str, file_name: str) -> str:
    """Generate a url that lets a user have download access to an s3 object for
    a limited amount of time."""

    params = {
        "Bucket": bucket,
        "Key": key,
        "ResponseContentDisposition": "attachment; {}".format(file_name)
    }

    s3_client = boto3.client('s3')
    return s3_client.generate_presigned_url(
        ClientMethod="get_object",
        Params=params,
        ExpiresIn=300
    )


# @cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def get_sensor_metadata(network: str, sensor: str = None) -> Response:
    """Return metadata for all sensors within a network. Sensors can also be
    be filtered by various other properties. If no single sensor is specified,
    the default is to return metadata for all sensors within the network.

    :endpoint: /sensor-networks/<network_name>/sensors/<sensor>"""

    args = request.args.to_dict()
    args.update({"network": network, "sensors": [sensor]})

    validator = Validator()
    validated = validator.load(args)
    if validated.errors:
        return bad_request(validated.errors)

    network = validated.data["network"]
    sensors = validated.data["sensors"]

    if sensor is None:
        sensors = []
        for node in NodeMeta.all(network.name):
            sensors += node.sensors
    elif sensor.lower() not in network.sensors():
        return bad_request("Invalid sensor {} for {}".format(sensor, network))

    geojson = validated.data.get('geom')

    if geojson:

        nodes_within_geom = NodeMeta.within_geojson(network, geojson).all()
        if not nodes_within_geom:
            return bad_request("No sensors found within {}!".format(geojson))

        sensors_within_geom = NodeMeta.sensors_from_nodes(nodes_within_geom)
        if sensor is None:
            sensors = sensors_within_geom
        elif SensorMeta.query.get(sensor) not in sensors_within_geom:
            return bad_request("No sensors found within {}!".format(geojson))

    result = [format_sensor_metadata(s) for s in set(sensors)]
    return jsonify(json_response_base(validated, result, args))


# @cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def get_feature_metadata(network: str, feature: str = None) -> Response:
    """Return metadata about features for some network. If no feature is
    specified, return metadata about all features within the network.

    :endpoint: /sensor-networks/<network_name>/features_of_interest/<feature>"""

    args = request.args.to_dict()
    args.update({"network": network, "features": [feature]})

    validator = Validator()
    validated = validator.load(args)
    if validated.errors:
        return bad_request(validated.errors)

    network = validated.data["network"]
    features = validated.data["features"]

    if feature is None:
        features = []
        for f in network.features():
            features.append(FeatureMeta.query.get(f))
    elif all(feature.lower() not in f for f in network.features()):
        return bad_request("Invalid feature {} for {}".format(feature, network))

    geojson = validated.data.get('geom')

    if geojson:

        nodes_within_geom = NodeMeta.within_geojson(network, geojson).all()
        if not nodes_within_geom:
            return bad_request("No features found within {}!".format(geojson))

        feature_set = set()
        for node in nodes_within_geom:
            feature_set.update(node.features())

        if feature is None:
            features = set(FeatureMeta.query.get(f.split('.')[0]) for f in feature_set)
        elif feature not in feature_set:
            return bad_request("No features found within {}!".format(geojson))

    result = [format_feature_metadata(f) for f in features]
    return jsonify(json_response_base(validated, result, args))


@crossdomain(origin="*")
def check(network: str) -> Response:
    """Validate query parameters.

    :endpoint: /sensor-networks/<network-name>/check"""

    nodes = request.args.get("node") or request.args.get("nodes")
    sensors = request.args.get("sensor") or request.args.get("sensors")
    features = request.args.get("feature") or request.args.get("features")

    args = {
        "network": network,
        "features": features.split(",") if features else [],
        "nodes": nodes.split(",") if nodes else [],
        "sensors": sensors.split(",") if sensors else [],
    }

    validator = Validator()
    validated = validator.load(args)
    if validated.errors:
        return bad_request(validated.errors)

    return jsonify({'message': 'Your query params are good to go.'})


@crossdomain(origin="*")
def get_observations(network: str) -> Response:
    """Return raw sensor network observations for a single feature within
    the specified network.

    :endpoint: /sensor-networks/<network-name>/query?feature=<feature>"""

    nodes = request.args.get("nodes")
    sensors = request.args.get("sensors")
    feature = request.args.get("feature")

    args = request.args.to_dict()
    args.update({
        "network": network,
        "feature": feature,
        "nodes": nodes.split(",") if nodes else [],
        "sensors": sensors.split(",") if sensors else [],
    })

    validator = Validator()
    validated = validator.load(args)
    if validated.errors:
        return bad_request(validated.errors)

    if '.' in feature:
        feature, property_ = feature.split('.', 1)
        validated.data.update({'property': property_})

    redshift_base.metadata.reflect()
    table = redshift_base.metadata.tables[network + "__" + feature]

    try:
        query = observation_query(table, **validated.data)
    except KeyError as err:
        return bad_request(str(err))

    data = list()
    for obs in query:
        data.append(format_observation(obs, table))

    return jsonify(json_response_base(validated, data, args))


@crossdomain(origin="*")
def get_observation_nearest(network: str) -> Response:
    """Return a single observation from the node nearest to the specified
    long, lat coordinate.

    :endpoint: /sensor-networks/<network-name>/nearest
               ?lng=<lng>&lat=<lat>&feature=<feature>"""

    args = request.args.to_dict()
    args.update({"network": network})

    validator = NearestValidator(only=('lat', 'lng', 'feature', 'network', 'datetime'))
    validated = validator.load(args)
    if validated.errors:
        return bad_request(validated.errors)

    result = get_observation_nearest_query(validated)
    return jsonify(json_response_base(validated, [result], args))


@crossdomain(origin="*")
def get_observations_download(network: str) -> Response:
    """Stream a sensor network's bulk records to a csv file.

    :endpoint: /sensor-networks/<network>/download"""

    nodes = request.args.get("node") or request.args.get("nodes")
    sensors = request.args.get("sensor") or request.args.get("sensors")
    features = request.args.get("feature") or request.args.get("features")

    kwargs = request.args.to_dict()
    kwargs.update({
        "network": network,
        "features": features.split(",") if features else [],
        "nodes": nodes.split(",") if nodes else [],
        "sensors": sensors.split(",") if sensors else [],
    })

    validator = NoLimitValidator()
    deserialized = validator.load(kwargs)
    if deserialized.errors:
        return bad_request(deserialized.errors)

    if not kwargs["features"]:
        kwargs["features"] = deserialized.data['network'].features()

    deserialized = validator.load(kwargs)

    if not deserialized.data.get('start_datetime'):
        deserialized.data.update({'start_datetime': datetime.now() - timedelta(days=7)})

    if deserialized.data.get('data_type') == 'json':
        stream = get_observation_datadump_json(**deserialized.data)
        filename = datetime.now().isoformat() + '-' + deserialized.data["network"].name + '.json'
        attachment = Response(stream_with_context(stream), mimetype="text/json")
    else:
        stream = get_observation_datadump_csv(**deserialized.data)
        filename = datetime.now().isoformat() + '-' + deserialized.data["network"].name + '.csv'
        attachment = Response(stream_with_context(stream), mimetype="text/csv")

    content_disposition = 'attachment; filename={}'.format(filename)
    attachment.headers["Content-Disposition"] = content_disposition
    return attachment


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def get_aggregations(network: str) -> Response:
    """Aggregate individual node observations up to larger units of time.
    Do so by applying aggregate functions on all observations found within
    a specified window of time.

    :endpoint: /sensor-networks/<network-name>/aggregate"""

    node = request.args.get("node")
    feature = request.args.get("feature")
    sensors = request.args.get("sensor") or request.args.get("sensors")

    args = request.args.to_dict()
    args.update({
        "network": network,
        "feature": feature,
        "node": node,
        "sensors": sensors.split(",") if sensors else [],
    })

    validator = AggregateValidator()
    validated = validator.load(args)
    if validated.errors:
        return bad_request(validated.errors)

    if '.' in feature:
        validated.data.update({'property': feature.split('.', 1)[-1]})

    try:
        aggregate_fn = aggregate_fn_map[validated.data.get("function")]
        result = aggregate_fn(validated.data)
    except ValueError as err:
        return bad_request(str(err))
    return jsonify(json_response_base(validated, result, args))


def observation_query(table, **kwargs):
    """Constructs a query used to fetch raw data from a Redshift table. Used
    by the /query and /download endpoints."""

    nodes = [n.id for n in kwargs.get("nodes")]
    sensors = [s.name for s in kwargs.get("sensors")]
    limit = kwargs.get("limit")
    offset = kwargs.get("offset")
    start_dt = kwargs.get("start_datetime")
    end_dt = kwargs.get("end_datetime")
    condition = kwargs.get("filter")
    property_ = kwargs.get("property")

    q = redshift_session.query(table)
    q = q.filter(table.c.datetime >= start_dt) if start_dt else q
    q = q.filter(table.c.datetime < end_dt) if end_dt else q
    q = q.filter(sqla_fn.lower(table.c.node_id).in_(nodes)) if nodes else q
    q = q.filter(sqla_fn.lower(table.c.sensor).in_(sensors)) if sensors else q
    try:
        q = q.filter(table.c[property_] != None) if property_ else q
    except KeyError:
        raise ValueError('Bad property name {}'.format(property_))
    q = q.filter(condition) if condition is not None else q
    q = q.order_by(desc(table.c.datetime))
    q = q.limit(limit) if limit else q
    q = q.offset(offset) if offset else q

    return q


def format_network_metadata(network):
    """Response format for network metadata.

    :param network: (Row) sensor__network_metadata object
    :returns: (dict) formatted result"""

    network_response = {
        'name': network.name,
        'features': sorted(list(network.features())),
        'nodes': sorted([n for n in network.tree()]),
        'sensors': sorted(list(network.sensors())),
        'info': network.info
    }

    return network_response


def format_node_metadata(node):
    """Response format for network metadata.

    :param node: (Row) sensor__node_metadata object
    :returns: (dict) formatted result"""

    node_response = {
        "type": "Feature",
        'geometry': {
            "type": "Point",
            "coordinates": [
                wkb.loads(bytes(node.location.data)).x,
                wkb.loads(bytes(node.location.data)).y
            ],
        },
        "properties": {
            "id": node.id,
            "network": node.sensor_network,
            "sensors": [sensor.name for sensor in node.sensors],
            "info": node.info,
        },
    }

    return node_response


def format_sensor_metadata(sensor):
    """Response format for network metadata.

    :param sensor: (Row) sensor__sensors object
    :returns: (dict) formatted result"""

    sensor_response = {
        'name': sensor.name,
        'properties': list(sensor.observed_properties.values()),
        'info': sensor.info
    }

    return sensor_response


def format_feature_metadata(feature):
    """Response format for network metadata.

    :param feature: (Row) sensor__features_of_interest object
    :returns: (dict) formatted result"""

    feature_response = {
        'name': feature.name,
        'properties': feature.observed_properties,
    }

    return feature_response


def format_observation(obs, table):
    """Response format for a feature observation.

    :param obs: (Row) row from a redshift table for a single feature
    :param table: (SQLAlchemy.Table) table object for a single feature
    :returns: (dict) formatted result"""

    obs_response = {
        'node': obs.node_id,
        'meta_id': obs.meta_id,
        'datetime': obs.datetime.isoformat().split('+')[0],
        'sensor': obs.sensor,
        'feature': table.name.split('__', 1)[-1],
        'results': {}
    }

    meta_properties = {'node_id', 'datetime', 'sensor', 'meta_id'}
    all_properties = set([c.name for c in table.c])
    for prop in all_properties - meta_properties:
        obs_response['results'][prop] = getattr(obs, prop)

    return obs_response


def get_observation_queries(args):
    """Queries used to get raw feature of interest rows from Redshift."""

    tables = []
    network = args.data['network']
    features = args.data['features']

    for feature in features:
        table_name = network.name + '__' + feature.name
        table = reflect(table_name, MetaData(), redshift_engine)
        tables.append(table)

    return [(observation_query(table, **args.data), table) for table in tables]


def get_observation_nearest_query(args):
    """Get an observation of the specified feature from the node nearest
    to the provided long, lat coordinates.

    :param args: (ValidatorResult) validated query arguments
    """

    lng = args.data["lng"]
    lat = args.data["lat"]
    feature = args.data["feature"]
    network = args.data["network"]
    point_dt = args.data["datetime"] if args.data.get('datetime') else datetime.now()

    nearest_nodes_rp = NodeMeta.nearest_neighbor_to(
        lng, lat, network=network.name, features=[feature.name]
    )

    if not nearest_nodes_rp:
        return "No nodes could be found nearby with your target feature."

    feature = reflect(network + '__' + feature.name, MetaData(), redshift_engine)

    result = None
    for row in nearest_nodes_rp:
        result = redshift_session.query(feature).filter(and_(
            feature.c.node_id == row.node,
            feature.c.datetime <= point_dt + timedelta(hours=12),
            feature.c.datetime >= point_dt - timedelta(hours=12)
        )).order_by(
            asc(
                # Ensures that the interval values is always positive,
                # since the abs() function doesn't work for intervals
                sqla_fn.greatest(point_dt, feature.c.datetime) -
                sqla_fn.least(point_dt, feature.c.datetime)
            )
        ).first()

        if result is not None:
            break

    if result is None:
        return "Your feature has not been reported on by the nearest 10 " \
               "nodes at the time provided."
    return format_observation(result, feature)


def get_observation_datadump_csv(**kwargs):
    """Query and yield chunks of sensor network observations for streaming."""

    class ValidatorResultProxy(object):
        pass
    vr_proxy = ValidatorResultProxy()
    vr_proxy.data = kwargs

    queries_and_tables = get_observation_queries(vr_proxy)

    chunksize = 100

    buffer = io.StringIO()
    writer = csv.writer(buffer)

    for query, table in queries_and_tables:

        writer.writerow([c.name for c in table.c])
        rownum = 1

        # Chunk sizes of 25000 seem to be the sweet spot. The windowed_query
        # doesn't take that long to calculate the query bounds, and the results
        # are large enough that download speed is decent
        for row in query.yield_per(1000):

            rownum += 1
            writer.writerow([getattr(row, c) for c in row.keys()])

            if rownum % chunksize == 0:
                yield buffer.getvalue()
                buffer.close()
                buffer = io.StringIO()
                writer = csv.writer(buffer)

        writer.writerow([])

    yield buffer.getvalue()
    buffer.close()


def get_observation_datadump_json(**kwargs):
    """Query and yield chunks of sensor network observations for streaming."""

    class ValidatorResultProxy(object):
        pass
    vr_proxy = ValidatorResultProxy()
    vr_proxy.data = kwargs

    queries_and_tables = get_observation_queries(vr_proxy)

    chunksize = 1000
    buffer = '{"objects": ['

    for query, table in queries_and_tables:
        columns = [c.name for c in table.c]

        for i, row in enumerate(query.yield_per(1000)):
            row = dict(zip(columns, row))
            buffer += json.dumps(row, default=unknown_object_json_handler)
            buffer += ","

            if i % chunksize == 0:
                yield buffer
                buffer = ""

    # Remove the trailing comma and close the json
    yield buffer.rsplit(',', 1)[0] + ']}'


def get_raw_metadata():

    pass


def sanitize_validated_args():

    pass


from plenario.sensor_network.api.sensor_response import json_response_base, bad_request
from plenario.models.SensorNetwork import NetworkMeta, NodeMeta, FeatureMeta, SensorMeta
from plenario.sensor_network.api.sensor_aggregate_functions import aggregate_fn_map
