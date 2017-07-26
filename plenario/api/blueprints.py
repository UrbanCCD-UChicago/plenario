import json
from time import sleep

from flask import Blueprint, make_response

from plenario.sensor_network.api.ifttt import get_ifttt_meta, get_ifttt_observations, ifttt_status, ifttt_test_setup
from plenario.sensor_network.api.sensor_networks import check, get_aggregations, get_feature_metadata, \
    get_network_map, get_network_metadata, get_node_download, get_node_metadata, get_observation_nearest, \
    get_observations, get_observations_download, get_sensor_metadata
from .common import cache, make_cache_key
from .point import datadump_view, dataset_fields, detail, detail_aggregate, get_job_view, grid, meta
from .sensor import weather, weather_fill, weather_stations
from .shape import aggregate_point_data, export_shape, get_all_shape_datasets
from .timeseries import timeseries


api = Blueprint('api', __name__)

API_VERSION = '/v1'

prefix = API_VERSION + '/api'


api.add_url_rule('{}{}'.format(prefix, '/timeseries'), 'timeseries', timeseries)
api.add_url_rule('{}{}'.format(prefix, '/detail'), 'detail', detail)
api.add_url_rule('{}{}'.format(prefix, '/detail-aggregate'), 'detail-aggregate', detail_aggregate)
api.add_url_rule('{}{}'.format(prefix, '/datasets'), 'meta', meta)
api.add_url_rule('{}{}'.format(prefix, '/fields/<dataset_name>'), 'point_fields', dataset_fields)
api.add_url_rule('{}{}'.format(prefix, '/grid'), 'grid', grid)

api.add_url_rule('{}{}'.format(prefix, '/weather/<table>/'), 'weather', weather)
api.add_url_rule('{}{}'.format(prefix, '/weather-stations/'), 'weather_stations', weather_stations)
api.add_url_rule('/secret/weather/fill/', 'weather_fill', weather_fill)

api.add_url_rule('{}{}'.format(prefix, '/shapes/'), 'shape_index', get_all_shape_datasets)
api.add_url_rule('{}{}'.format(prefix, '/shapes/<dataset_name>'), 'shape_export', export_shape)
api.add_url_rule('{}{}'.format(prefix, '/shapes/<polygon_dataset_name>/<point_dataset_name>'), 'aggregate', aggregate_point_data)

api.add_url_rule('{}{}'.format(prefix, '/jobs/<ticket>'), view_func=get_job_view, methods=['GET'])

api.add_url_rule('{}{}'.format(prefix, '/datadump'), 'datadump', datadump_view)

# sensor networks
api.add_url_rule('{}{}'.format(prefix, '/sensor-networks'), 'sensor_networks', get_network_metadata)
api.add_url_rule('{}{}'.format(prefix, '/sensor-networks/<network>'), 'sensor_network', get_network_metadata)
api.add_url_rule('{}{}'.format(prefix, '/sensor-networks/<network>/query'), 'observations', get_observations)
api.add_url_rule('{}{}'.format(prefix, '/sensor-networks/<network>/aggregate'), 'node_aggregate', get_aggregations)
api.add_url_rule('{}{}'.format(prefix, '/sensor-networks/<network>/download'), 'sensor_network_download', get_observations_download)
api.add_url_rule('{}{}'.format(prefix, '/sensor-networks/<network>/map'), 'sensor_network_map', get_network_map)
api.add_url_rule('{}{}'.format(prefix, '/sensor-networks/<network>/nearest'), 'nearest', get_observation_nearest)
api.add_url_rule('{}{}'.format(prefix, '/sensor-networks/<network>/check'), 'check', check)

api.add_url_rule('{}{}'.format(prefix, '/sensor-networks/<network>/nodes'), 'network_nodes', get_node_metadata)
api.add_url_rule('{}{}'.format(prefix, '/sensor-networks/<network>/nodes/<node>'), 'single_node', get_node_metadata)
api.add_url_rule('{}{}'.format(prefix, '/sensor-networks/<network>/nodes/<node>/download'), 'node_download', get_node_download)

api.add_url_rule('{}{}'.format(prefix, '/sensor-networks/<network>/features'), 'features', get_feature_metadata)
api.add_url_rule('{}{}'.format(prefix, '/sensor-networks/<network>/features/<feature>'), 'features', get_feature_metadata)

api.add_url_rule('{}{}'.format(prefix, '/sensor-networks/<network>/sensors'), 'sensors', get_sensor_metadata)
api.add_url_rule('{}{}'.format(prefix, '/sensor-networks/<network>/sensors/<sensor>'), 'sensors', get_sensor_metadata)

# IFTTT
api.add_url_rule('/ifttt/v1/status', 'ifttt_status', ifttt_status)
api.add_url_rule('/ifttt/v1/test/setup', 'ifttt_test_setup', ifttt_test_setup, methods=['POST'])
api.add_url_rule('/ifttt/v1/triggers/property_comparison', 'ifttt_obs', get_ifttt_observations, methods=['POST'])
api.add_url_rule('/ifttt/v1/triggers/property_comparison/fields/<field>/options', 'ifttt_meta', get_ifttt_meta, methods=['POST'])


@api.route('{}{}'.format(prefix, '/flush-)cache'))
def flush_cache():
    cache.clear()
    resp = make_response(json.dumps({'status': 'ok', 'message': 'cache flushed!'}))
    resp.headers['Content-Type'] = 'application/json'
    return resp


@api.route('{}{}'.format(prefix, '/slow'))
@cache.cached(timeout=60 * 60 * 6, key_prefix=make_cache_key)
def slow():
    sleep(5)
    return 'I feel well rested'
