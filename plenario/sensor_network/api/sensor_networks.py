import csv
import io
import json
from collections import OrderedDict
from datetime import datetime, timedelta

from dateutil.parser import parse as dt_parse
from flask import request, make_response, Response, stream_with_context
from shapely import wkb
from sqlalchemy import MetaData, Table, func as sqla_fn, and_, asc

from plenario.api.common import cache, crossdomain
from plenario.api.common import make_cache_key, unknown_object_json_handler
from plenario.database import windowed_query
from plenario.utils.helpers import reflect

# Cache timeout of 5 minutes
CACHE_TIMEOUT = 60 * 10


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def get_network_metadata(network=None):
    """Return metadata for some network. If no network_name is specified, the
    default is to return metadata for all sensor networks.

    :endpoint: /sensor-networks/<network-name>
    :param network: (str) network name
    :returns: (json) response"""

    args = {"network": network.lower() if network else None}

    fields = ('network',)
    validated_args = sensor_network_validate(SensorNetworkValidator(only=fields), args)
    if validated_args.errors:
        return bad_request(validated_args.errors)

    return get_metadata("network", validated_args)


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def get_node_metadata(network, node=None):
    """Return metadata about nodes for some network. If no node_id or
    location_geom__within is specified, the default is to return metadata
    for all nodes within the network.

    :endpoint: /sensor-networks/<network-name>/nodes/<node>
    :param network: (str) network that exists in sensor__network_metadata
    :param node: (str) node that exists in sensor__node_metadata
    :returns: (json) response"""

    args = dict(request.args.to_dict(), **{
        "network": network,
        "nodes": [node.lower()] if node else None
    })

    fields = ('network', 'nodes', 'geom')
    validated_args = sensor_network_validate(SensorNetworkValidator(only=fields), args)
    if validated_args.errors:
        return bad_request(validated_args.errors)
    validated_args = sanitize_validated_args(validated_args)

    return get_metadata("nodes", validated_args)


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def get_sensor_metadata(network, sensor=None):
    """Return metadata for all sensors within a network. Sensors can also be
    be filtered by various other properties. If no single sensor is specified,
    the default is to return metadata for all sensors within the network.

    :endpoint: /sensor-networks/<network_name>/sensors/<sensor>
    :param network: (str) name from sensor__network_metadata
    :param sensor: (str) name from sensor__sensors
    :returns: (json) response"""

    args = dict(request.args.to_dict(), **{
        "network": network,
        "sensors": [sensor.lower()] if sensor else None
    })

    fields = ('network', 'sensors', 'geom')
    validated_args = sensor_network_validate(SensorNetworkValidator(only=fields), args)
    if validated_args.errors:
        return bad_request(validated_args.errors)
    validated_args = sanitize_validated_args(validated_args)

    return get_metadata("sensors", validated_args)


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def get_feature_metadata(network, feature=None):
    """Return metadata about features for some network. If no feature is
    specified, return metadata about all features within the network.

    :endpoint: /sensor-networks/<network_name>/features_of_interest/<feature>
    :param network: (str) network name from sensor__network_metadata
    :param feature: (str) name from sensor__features_of_interest
    :returns: (json) response"""

    args = dict(request.args.to_dict(), **{
        "network": network,
        "features": [feature.lower()] if feature else None
    })

    fields = ('network', 'features', 'geom')
    validated_args = sensor_network_validate(SensorNetworkValidator(only=fields), args)
    if validated_args.errors:
        return bad_request(validated_args.errors)

    return get_metadata("features", validated_args)


@crossdomain(origin="*")
def get_observations(network):
    """Return raw sensor network observations for a single feature within
    the specified network.

    :endpoint: /sensor-networks/<network-name>/query?feature=<feature>
    :param network: (str) network name
    :returns: (json) response"""

    args = dict(request.args.to_dict(), **{
        "network": network,
        "nodes": request.args["nodes"].split(",") if request.args.get("nodes") else None,
        "sensors": request.args["sensors"].split(",") if request.args.get("sensors") else None
    })
    args = sanitize_args(args)

    fields = ('network', 'nodes', 'start_datetime', 'end_datetime', 'geom',
              'feature', 'sensors', 'limit', 'offset', 'filter')
    validated_args = sensor_network_validate(RequiredFeatureValidator(only=fields), args)
    if validated_args.errors:
        return bad_request(validated_args.errors)
    validated_args.data.update({
        "features": [validated_args.data["feature"]],
        "feature": None
    })
    validated_args = sanitize_validated_args(validated_args)

    observation_queries = get_observation_queries(validated_args)
    if type(observation_queries) != list:
        return observation_queries

    return run_observation_queries(validated_args, observation_queries)


@crossdomain(origin="*")
def get_observation_nearest(network):
    """Return a single observation from the node nearest to the specified
    long, lat coordinate.

    :endpoint: /sensor-networks/<network-name>/near-me?lng=<lng>&lat=<lat>&feature=<feature>
    :param network: (str) network name
    :returns: (json) response"""

    args = dict(request.args.to_dict(), **{"network": network})
    args = sanitize_args(args)

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

    request_args = sanitize_args(request_args)
    validated_args = sensor_network_validate(NodeAggregateValidator(only=fields), request_args)
    if validated_args.errors:
        return bad_request(validated_args.errors)
    validated_args = sanitize_validated_args(validated_args)

    try:
        result = aggregate_fn_map[validated_args.data.get("function")](validated_args)
    except ValueError as err:
        return bad_request(err.message)
    return jsonify(validated_args, result, 200)


def observation_query(args, table):
    """Constructs a query used to fetch raw data from a Redshift table. Used
    by the /query and /download endpoints.

    :param args: (ValidatorResult) contains arguments in the data property
    :param table: (SQLAlchemy.Table) represents a database table
    :param condition: asdkfjhgasdfkjhgasdkfjhgasdfkjhgasdf"""

    nodes = args.data.get("nodes")
    start_dt = args.data.get("start_datetime")
    end_dt = args.data.get("end_datetime")
    sensors = args.data.get("sensors")
    limit = args.data.get("limit")
    offset = args.data.get("offset")
    condition = parse_tree(table, args.data.get("filter")) if args.data.get("filter") else None

    q = redshift_session.query(table)
    q = q.filter(table.c.datetime >= start_dt)
    q = q.filter(table.c.datetime < end_dt)

    q = q.filter(sqla_fn.lower(table.c.node_id).in_(nodes)) if nodes else q
    q = q.filter(sqla_fn.lower(table.c.sensor).in_(sensors)) if sensors else q
    q = q.filter(condition) if condition is not None else q
    q = q.limit(limit) if limit else q
    q = q.offset(offset) if offset else q

    return q


def get_raw_metadata(target, args):
    """Returns all valid metadata rows for a target metadata table given args.

    :param target: (str) which kind of metadata to return rows for
    :param args: (ValidatorResult) validated query arguments
    :returns: (list) of row proxies
              (Response) 400 for a query that would lead to nothing"""

    metadata_args = {
        "target": target,
        "network": args.data.get("network"),
        "nodes": args.data.get("nodes"),
        "sensors": args.data.get("sensors"),
        "features": args.data.get("features"),
        "geom": args.data.get("geom")
    }
    return metadata(**metadata_args)


def get_metadata(target, args):
    """Returns all valid metadata for a target metadata table given args. The
    results are formatted and turned into a response object.

    :param target: (str) which kind of metadata to return rows for
    :param args: (ValidatorResult) validated query arguments
    :returns: (Response) 200 containing valid metadata rows
                         400 for a query that would lead to nothing"""

    args = remove_null_keys(args)
    raw_metadata = get_raw_metadata(target, args)
    if type(raw_metadata) != list:
        return raw_metadata
    return jsonify(args, [format_metadata[target](record) for record in raw_metadata], 200)


def format_network_metadata(network):
    """Response format for network metadata.

    :param network: (Row) sensor__network_metadata object
    :returns: (dict) formatted result"""

    network_response = {
        'name': network.name,
        'features': FeatureMeta.index(network.name),
        'nodes': NodeMeta.index(network.name),
        'sensors': SensorMeta.index(network.name),
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


# format_metadata
# ---------------
# mapping of formatting methods to keys for use in the get_raw_metadata method
format_metadata = {
    "network": format_network_metadata,
    "nodes": format_node_metadata,
    "sensors": format_sensor_metadata,
    "features": format_feature_metadata
}


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

    args = sanitize_validated_args(args)

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


def run_observation_queries(args, queries):
    """Run a list of queries, collect results, and return formatted JSON.

    :param args: (ValidatorResult) validated query arguments
    :param queries: (list) of SQLAlchemy query objects
    :returns: (Response) containing rows formatted into JSON"""

    data = list()
    for query, table in queries:
        data += [format_observation(obs, table) for obs in query.all()]

    remove_null_keys(args)
    if 'geom' in args.data:
        args.data.pop('geom')
    data.sort(key=lambda x: dt_parse(x["datetime"]))

    return jsonify(args, data, 200)


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


def metadata(target, network=None, nodes=None, sensors=None, features=None, geom=None):
    """Given a set of sensor network metadata, determine which target metadata
    rows are valid, if any.

    :param target: (str) which metadata type to return
    :param network: (str) name of the network metadata
    :param nodes: (list) containing node ids
    :param sensors: (list) containing sensor names
    :param features: (list) conatining feature names
    :param geom: (str) containing GeoJSON location constraint
    :returns: (list) of row objects containing sensor network metadata
              (Response) 400 returns a message for no valid values"""

    meta_levels = OrderedDict([
        ("network", network),
        ("nodes", nodes),
        ("sensors", sensors),
        ("features", features),
    ])

    for i, key in enumerate(meta_levels):
        current_state = list(meta_levels.items())
        value = meta_levels[key]

        if key == "network":
            meta_levels[key] = filter_meta(key, [], value, geom)
        else:
            meta_levels[key] = filter_meta(key, current_state[i - 1], value, geom)

        if not meta_levels[key]:
            msg = "Given your parameters, we could not find your target {} " \
                  "within {}, {}".format(
                      target,
                      current_state[i - 1][0],
                      current_state[i - 1][1]
                  )
            try:
                return bad_request(msg)
            except RuntimeError:
                raise ValueError(msg)

        if key == target:
            return meta_levels[key]


def filter_meta(meta_level, upper_filter_values, filter_values, geojson):
    """Establishes valid metadata at any given metadata level. For example,
    given a set of nodes, which are the valid sensors. Given a set of sensors,
    which are the valid features.

    :param meta_level: (str) where we are in the metadata heirarchy
    :param upper_filter_values: (list) of row objects for the level above
    :param filter_values: (list) of strings the filter the current level by
    :param geojson: (str) GeoJSON for filtering nodes
    :return: (list) of valid row objects for the current level"""

    meta_queries = {
        "network": (session.query(NetworkMeta), NetworkMeta),
        "nodes": (session.query(NodeMeta), NodeMeta),
        "sensors": (session.query(SensorMeta), SensorMeta),
        "features": (session.query(FeatureMeta), FeatureMeta)
    }

    query, table = meta_queries[meta_level]
    upper_filter_values = upper_filter_values[1] if upper_filter_values else None

    valid_values = []
    if meta_level == "nodes":
        for network in upper_filter_values:
            valid_values += [node.id for node in network.nodes]
        if geojson:
            geom = NodeMeta.location.ST_Within(sqla_fn.ST_GeomFromGeoJSON(geojson))
            query = query.filter(geom)
    elif meta_level == "sensors":
        for node in upper_filter_values:
            valid_values += [sensor.name for sensor in node.sensors]
    elif meta_level == "features":
        for sensor in upper_filter_values:
            valid_values += [p.split(".")[0] for p in list(sensor.observed_properties.values())]

    if type(filter_values) != list and filter_values is not None:
        filter_values = [filter_values]

    if meta_level == "network" and not filter_values:
        return query.all()
    if meta_level == "network" and filter_values:
        return query.filter(table.name.in_(filter_values)).all()

    if not filter_values and valid_values:
        filter_values = valid_values
    else:
        filter_values = set(filter_values).intersection(valid_values)

    try:
        return query.filter(table.name.in_(filter_values)).all()
    except AttributeError:
        return query.filter(table.id.in_(filter_values)).all()


def jsonify(args, data, status_code):
    """Returns a JSON response, I prefer this to the flask provided one because
    it doesn't sort the keys. Meaning we can keep the meta header at the top,
    which feels alot better.

    :param args: (ValidatorResult) validated query arguements
    :param data: (list) of json formatted results
    :param status_code: (int) response status code
    :returns: (Response) HTTP reponse containing JSON"""

    resp = json_response_base(args, data, args.data)
    resp = make_response(json.dumps(resp, default=unknown_object_json_handler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp


def remove_null_keys(args):
    """Helper method that removes null query parameters for cleanliness.
    :returns: (dict) cleaned up query arguments"""

    null_keys = [k for k in args.data if args.data[k] is None]
    for key in null_keys:
        del args.data[key]
    return args


def sanitize_args(args):
    """Helper method that removes that makes centain query parameters play nice
    with our validator and queries.

    :returns: (dict) cleaned up query arguments"""

    for k in args:
        try:
            args[k] = args[k].lower()
        except AttributeError:
            continue
        if k in {"nodes", "sensors", "features"}:
            args[k] = args[k].split(",")
        if "+" in args[k]:
            args[k] = args[k].split("+")[0]
    return args


def sanitize_validated_args(args):
    """Helper method that makes validated query parameters play nice with
    queries.

    :returns: (dict) cleaned up query arguments"""

    args = remove_null_keys(args)
    for k in args.data:
        try:
            args.data[k] = args.data[k].lower()
        except AttributeError:
            continue
        if k in {"nodes", "sensors", "features"}:
            args.data[k] = args.data[k].split(",")
        if "+" in args.data[k]:
            args.data[k] = args.data[k].split("+")[0]
    return args


from plenario.database import session, redshift_session, redshift_engine
from plenario.sensor_network.api.sensor_response import json_response_base, bad_request
from plenario.api.validator import SensorNetworkValidator, DatadumpValidator, sensor_network_validate
from plenario.sensor_network.api.sensor_validator import NodeAggregateValidator, RequiredFeatureValidator
from plenario.sensor_network.api.sensor_validator import NearMeValidator
from plenario.models.SensorNetwork import NetworkMeta, NodeMeta, FeatureMeta, SensorMeta
from plenario.sensor_network.api.sensor_aggregate_functions import aggregate_fn_map
from plenario.api.condition_builder import parse_tree
