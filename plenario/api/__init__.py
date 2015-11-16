import json
from flask import make_response, Blueprint
from point import timeseries, detail, meta, dataset_fields, grid
from common import cache
from shape import get_all_shape_datasets, find_intersecting_shapes, export_shape
from sensor import weather_stations, weather

API_VERSION = '/v1'

api = Blueprint('api', __name__)
prefix = API_VERSION + '/api'
api.add_url_rule(prefix + '/timeseries', 'timeseries', timeseries)
api.add_url_rule(prefix + '/detail', 'detail', detail)
api.add_url_rule(prefix + '/datasets', 'meta', meta)
api.add_url_rule(prefix + '/fields', 'point_fields', dataset_fields)
api.add_url_rule(prefix + '/grid', 'grid', grid)
api.add_url_rule(prefix + '/weather/<table>/', 'weather', weather)
api.add_url_rule(prefix + '/api/weather-stations/', 'weather_stations', weather_stations)
api.add_url_rule(prefix + '/shapes/', 'shape_index', get_all_shape_datasets)
api.add_url_rule(prefix + '/shapes/intersections/<geojson>', 'shape_intersections', find_intersecting_shapes)
api.add_url_rule(prefix + '/shapes/<dataset_name>', 'shape_export', export_shape)


@api.route(API_VERSION + '/api/flush-cache')
def flush_cache():
    cache.clear()
    resp = make_response(json.dumps({'status': 'ok', 'message': 'cache flushed!'}))
    resp.headers['Content-Type'] = 'application/json'
    return resp
