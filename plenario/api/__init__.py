import json
from flask import make_response, Blueprint
from point import timeseries, detail, meta, dataset_fields, grid, detail_aggregate, datadump, get_datadump, get_job_view
from common import cache, make_cache_key
from shape import get_all_shape_datasets,\
                    export_shape, aggregate_point_data
from time import sleep
from sensor import weather_stations, weather
from flask_login import login_required

from plenario.sensor_network.api.sensor_networks import get_network_metadata, get_node_metadata, \
    get_observations, get_features, get_sensors

API_VERSION = '/v1'

api = Blueprint('api', __name__)
prefix = API_VERSION + '/api'

api.add_url_rule(prefix + '/timeseries', 'timeseries', timeseries)
api.add_url_rule(prefix + '/detail', 'detail', detail)
api.add_url_rule(prefix + '/detail-aggregate', 'detail-aggregate', detail_aggregate)
api.add_url_rule(prefix + '/datasets', 'meta', meta)
api.add_url_rule(prefix + '/fields/<dataset_name>', 'point_fields', dataset_fields)
api.add_url_rule(prefix + '/grid', 'grid', grid)

api.add_url_rule(prefix + '/weather/<table>/', 'weather', weather)
api.add_url_rule(prefix + '/weather-stations/', 'weather_stations', weather_stations)

api.add_url_rule(prefix + '/shapes/', 'shape_index', get_all_shape_datasets)
api.add_url_rule(prefix + '/shapes/<dataset_name>', 'shape_export', export_shape)
api.add_url_rule(prefix + '/shapes/<polygon_dataset_name>/<point_dataset_name>', 'aggregate', aggregate_point_data)

api.add_url_rule(prefix + '/jobs/<ticket>', view_func=get_job_view, methods=['GET'])

api.add_url_rule(prefix + '/datadump', 'datadump', datadump)
api.add_url_rule(prefix + '/datadump/<ticket>', 'get_datadump', get_datadump)
# sensor networks
api.add_url_rule(prefix + '/sensor-networks', 'sensor_networks', get_network_metadata)
api.add_url_rule(prefix + '/sensor-networks/<network_name>', 'sensor_network', get_network_metadata)
api.add_url_rule(prefix + '/sensor-networks/nodes/<node_id>', 'single_node', get_node_metadata)
api.add_url_rule(prefix + '/sensor-networks/<network_name>/nodes', 'network_nodes', get_node_metadata)
api.add_url_rule(prefix + '/sensor-networks/<network_name>/nodes/<node_id>', 'single_node', get_node_metadata)

api.add_url_rule(prefix + '/sensor-networks/features-of-interest', 'features', get_features)
api.add_url_rule(prefix + '/sensor-networks/<network_name>/features-of-interest', 'features', get_features)
api.add_url_rule(prefix + '/sensor-networks/<network_name>/features-of-interest/<feature>', 'features', get_features)
api.add_url_rule(prefix + '/sensor-networks/features-of-interest/<feature>', 'features', get_features)

api.add_url_rule(prefix + '/sensor-networks/sensors', 'sensors', get_sensors)
api.add_url_rule(prefix + '/sensor-networks/sensors/<sensor>', 'sensors', get_sensors)
api.add_url_rule(prefix + '/sensor-networks/<network_name>/sensors', 'sensors', get_sensors)
api.add_url_rule(prefix + '/sensor-networks/<network_name>/sensors/<sensor>', 'sensors', get_sensors)
api.add_url_rule(prefix + '/sensor-networks/<network_name>/features-of-interest/<feature>/sensors', 'sensors', get_sensors)
api.add_url_rule(prefix + '/sensor-networks/<network_name>/features-of-interest/<feature>/sensors/<sensor>', 'sensors', get_sensors)
api.add_url_rule(prefix + '/sensor-networks/features-of-interest/<feature>/sensors', 'sensors', get_sensors)
api.add_url_rule(prefix + '/sensor-networks/features-of-interest/<feature>/sensors/<sensor>', 'sensors', get_sensors)

api.add_url_rule(prefix + '/sensor-networks/<network_name>/query', 'observations', get_observations)
api.add_url_rule(prefix + '/sensor-networks/<network_name>/nodes/query', 'observations', get_observations)
api.add_url_rule(prefix + '/sensor-networks/<network_name>/nodes/<node_id>/query', 'observations', get_observations)


@api.route(prefix + '/flush-cache')
@login_required
def flush_cache():
    cache.clear()
    resp = make_response(json.dumps({'status': 'ok', 'message': 'cache flushed!'}))
    resp.headers['Content-Type'] = 'application/json'
    return resp


@api.route(prefix + '/slow')
@cache.cached(timeout=60*60*6, key_prefix=make_cache_key)
def slow():
    sleep(5)
    return "I feel well rested"
