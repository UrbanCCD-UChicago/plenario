import csv
import io

from datetime import datetime, timedelta
from flask import request, Response, stream_with_context, jsonify
from marshmallow import Schema
from marshmallow.exceptions import ValidationError
from marshmallow.fields import Field, List, DateTime, Integer, String
from marshmallow.validate import Range
from shapely import wkb
from sqlalchemy import MetaData, Table, func as sqla_fn, and_, asc
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm.exc import NoResultFound

from plenario.api.common import cache, crossdomain
from plenario.api.common import make_cache_key
from plenario.database import windowed_query, redshift_engine
from plenario.utils.helpers import reflect

# Cache timeout of 5 minutes
CACHE_TIMEOUT = 60 * 10

# Reflect all Redshift tables
base = automap_base()
base.prepare(redshift_engine, reflect=True)
Redshift = {k: v for k, v in base.classes.items()}


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
            value = value.lower()
            query = FeatureMeta.query.filter(FeatureMeta.name == value)
            return query.one()
        except NoResultFound:
            raise ValidationError("{} does not exist".format(value))

    def _serialize(self, value, attr, obj):
        return value.name.lower()


class Validator(Schema):

    # geom
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

    filter = String(allow_none=True, missing=None, default=None)
    limit = Integer(missing=1000)
    offset = Integer(missing=0, validate=Range(0))


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
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

    return jsonify([format_network_metadata(n) for n in networks])


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
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
    elif node not in network.tree():
        return bad_request("Invalid node {} for {}".format(node, network))

    return jsonify([format_node_metadata(n) for n in nodes])


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
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
    elif sensor not in network.sensors():
        return bad_request("Invalid sensor {} for {}".format(sensor, network))

    return jsonify([format_sensor_metadata(s) for s in sensors])


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
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
    elif all(feature not in f for f in network.features()):
        return bad_request("Invalid feature {} for {}".format(feature, network))

    return jsonify([format_feature_metadata(f) for f in features])


@crossdomain(origin="*")
def get_observations(network):
    """Return raw sensor network observations for a single feature within
    the specified network.

    :endpoint: /sensor-networks/<network-name>/query?feature=<feature>
    :param network: (str) network name
    :returns: (json) response"""

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

    import pdb
    pdb.set_trace()

    table = Redshift[network + "__" + feature]
    query = observation_query(table, **validated.data)

    data = list()
    for obs in query:
        data += format_observation(obs, table)
    data.sort(key=lambda x: x["datetime"])

    return jsonify(data)


@crossdomain(origin="*")
def get_observation_nearest(network):
    """Return a single observation from the node nearest to the specified
    long, lat coordinate.

    :endpoint: /sensor-networks/<network-name>/near-me?lng=<lng>&lat=<lat>&feature=<feature>
    :param network: (str) network name
    :returns: (json) response"""

    args = dict(request.args.to_dict(), **{"network": network})

    fields = ('datetime', 'network', 'feature', 'lat', 'lng')
    validated_args = sensor_network_validate(NearMeValidator(only=fields), args)
    if validated_args.errors:
        return bad_request(validated_args.errors)

    result = get_observation_nearest_query(validated_args)
    return jsonify(validated_args, [result], 200)


@crossdomain(origin="*")
def get_observations_download(network):
    """Queue a datadump job for raw sensor network observations and return
    links to check on its status and eventual download. Has a longer cache
    timeout than the other endpoints -- datadumps are a lot of work.

    :endpoint: /sensor-networks/<network-name>/download
    :param network: (str) network name
    :returns: (json) response"""

    args = dict(request.args.to_dict(), **{
        "network": network,
        "nodes": request.args["nodes"].split(",") if request.args.get("nodes") else None,
        "sensors": request.args["sensors"].split(",") if request.args.get("sensors") else None,
        "features": request.args["features"].split(",") if request.args.get("features") else None
    })

    fields = ('network', 'nodes', 'start_datetime', 'end_datetime',
              'limit', 'geom', 'features', 'sensors', 'offset')
    validated_args = sensor_network_validate(DatadumpValidator(only=fields), args)
    if validated_args.errors:
        return bad_request(validated_args.errors)

    stream = get_observation_datadump_csv(**validated_args.data)

    network = validated_args.data["network"]
    fmt = "csv"  # validated_args.data["data_type"]
    content_disposition = 'attachment; filename={}.{}'.format(network, fmt)

    attachment = Response(stream_with_context(stream), mimetype="text/%s" % fmt)
    attachment.headers["Content-Disposition"] = content_disposition
    return attachment


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def get_aggregations(network):
    """Aggregate individual node observations up to larger units of time.
    Do so by applying aggregate functions on all observations found within
    a specified window of time.

    :endpoint: /sensor-networks/<network-name>/aggregate
    :param network: (str) from sensor__network_metadata
    :returns: (json) response"""

    fields = ("network", "node", "sensors", "feature", "function",
              "start_datetime", "end_datetime", "agg")

    request_args = dict(request.args.to_dict(), **{
        "network": network,
        "feature": request.args.get("feature").split(",") if request.args.get("feature") else None,
    })

    validated_args = sensor_network_validate(NodeAggregateValidator(only=fields), request_args)
    if validated_args.errors:
        return bad_request(validated_args.errors)

    try:
        result = aggregate_fn_map[validated_args.data.get("function")](validated_args)
    except ValueError as err:
        return bad_request(err.message)
    return jsonify(validated_args, result, 200)


def observation_query(table, **kwargs):
    """Constructs a query used to fetch raw data from a Redshift table. Used
    by the /query and /download endpoints."""

    nodes = kwargs.get("nodes")
    sensors = kwargs.get("sensors")
    limit = kwargs.get("limit")
    offset = kwargs.get("offset")

    start_dt = datetime.now() - timedelta(days=7) \
        if kwargs.get("start_datetime") is None \
        else kwargs["start_datetime"]
    end_dt = datetime.now() \
        if kwargs.get("end_datetime") is None \
        else kwargs["end_datetime"]
    condition = parse_tree(table, kwargs.get("filter")) \
        if kwargs.get("filter") \
        else None

    q = redshift_session.query(table)
    q = q.filter(table.datetime >= start_dt)
    q = q.filter(table.datetime < end_dt)

    q = q.filter(sqla_fn.lower(table.node_id).in_(nodes)) if nodes else q
    q = q.filter(sqla_fn.lower(table.sensor).in_(sensors)) if sensors else q
    q = q.filter(condition) if condition is not None else q
    q = q.limit(limit) if limit else q
    q = q.offset(offset) if offset else q

    return q


def get_raw_metadata(target, kwargs):
    """Returns all valid metadata rows for a target metadata table given args.

    :param target: (str) which kind of metadata to return rows for
    :param kwargs: (ValidatorResult) validated query arguments
    :returns: (list) of row proxies
              (Response) 400 for a query that would lead to nothing"""

    metadata_args = {
        "target": target,
        "network": kwargs.get("network"),
        "nodes": kwargs.get("nodes"),
        "sensors": kwargs.get("sensors"),
        "features": kwargs.get("features"),
        "geom": kwargs.get("geom")
    }
    return metadata(**metadata_args)


def get_metadata(target, args):
    """Returns all valid metadata for a target metadata table given args. The
    results are formatted and turned into a response object.

    :param target: (str) which kind of metadata to return rows for
    :param args: (ValidatorResult) validated query arguments"""

    return get_raw_metadata(target, args)


def format_network_metadata(network):
    """Response format for network metadata.

    :param network: (Row) sensor__network_metadata object
    :returns: (dict) formatted result"""

    network_response = {
        'name': network.name,
        'features': list(network.features()),
        'nodes': [n for n in network.tree()],
        'sensors': network.sensors(),
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
        'feature': table.name,
        'results': {}
    }

    for prop in (set([c.name for c in table.c]) - {'node_id', 'datetime', 'sensor', 'meta_id'}):
        obs_response['results'][prop] = getattr(obs, prop)

    return obs_response


def get_observation_queries(args):
    """Generate queries used to get raw feature of interest rows from Redshift.

    :param args: (ValidatorResult) validated query arguments
    :returns: (list) of SQLAlchemy query objects"""

    tables = []
    meta = MetaData()

    result = get_raw_metadata("features", args)
    if type(result) != list:
        return result

    for feature in result:
        tables.append(Table(
            feature.name, meta,
            autoload=True,
            autoload_with=redshift_engine
        ))

    return [(observation_query(args, table), table) for table in tables]


def get_observation_nearest_query(args):
    """Get an observation of the specified feature from the node nearest
    to the provided long, lat coordinates.

    :param args: (ValidatorResult) validated query arguments
    """

    lng = args.data["lng"]
    lat = args.data["lat"]
    feature = args.data["feature"].split(".")[0]
    properties = args.data["feature"]
    network = args.data["network"]
    point_dt = args.data["datetime"]

    if type(point_dt) != datetime:
        point_dt = dt_parse(point_dt)

    nearest_nodes_rp = NodeMeta.nearest_neighbor_to(
        lng, lat, network=network, features=[properties]
    )

    if not nearest_nodes_rp:
        return "No nodes could be found nearby with your target feature."

    feature = reflect(feature, MetaData(), redshift_engine)

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
    """Query and store large amounts of raw sensor network observations for
    download.

    :param args: (ValidatorResult) validated query arguments
    :returns (dict) containing URL to download chunked data"""

    class ValidatorResultProxy(object):
        pass
    vr_proxy = ValidatorResultProxy()
    vr_proxy.data = kwargs

    queries_and_tables = get_observation_queries(vr_proxy)

    rownum = 0
    chunksize = 10

    buffer = io.StringIO()
    writer = csv.writer(buffer)

    for query, table in queries_and_tables:

        writer.writerow([c.name for c in table.c])

        # Chunk sizes of 25000 seem to be the sweet spot. The windowed_query
        # doesn't take that long to calculate the query bounds, and the results
        # are large enough that download speed is decent
        for row in windowed_query(query, table.c.datetime, 25000):

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

def sanitize_validated_args():

    pass


from plenario.database import session, redshift_session, redshift_engine
from plenario.sensor_network.api.sensor_response import json_response_base, bad_request
from plenario.api.validator import SensorNetworkValidator, DatadumpValidator, sensor_network_validate
from plenario.sensor_network.api.sensor_validator import NodeAggregateValidator, RequiredFeatureValidator
from plenario.sensor_network.api.sensor_validator import NearMeValidator
from plenario.models.SensorNetwork import NetworkMeta, NodeMeta, FeatureMeta, SensorMeta
from plenario.sensor_network.api.sensor_aggregate_functions import aggregate_fn_map
from plenario.api.condition_builder import parse_tree
