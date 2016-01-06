import json
from flask.ext.cache import Cache
from plenario.settings import CACHE_CONFIG
from datetime import timedelta, date
from functools import update_wrapper
from flask import make_response, request, current_app
import csv
from shapely.geometry import asShape
from cStringIO import StringIO
from plenario.utils.helpers import get_size_in_degrees

cache = Cache(config=CACHE_CONFIG)

RESPONSE_LIMIT = 1000
CACHE_TIMEOUT = 60*60*6


def dthandler(obj):
    if isinstance(obj, date):
        return obj.isoformat()
    else:
        raise ValueError


# http://flask.pocoo.org/snippets/56/
def crossdomain(origin=None, methods=None, headers=None,
                max_age=21600, attach_to_all=True,
                automatic_options=True): # pragma: no cover
    if methods is not None:
        methods = ', '.join(sorted(x.upper() for x in methods))
    if headers is not None and not isinstance(headers, basestring):
        headers = ', '.join(x.upper() for x in headers)
    if not isinstance(origin, basestring):
        origin = ', '.join(origin)
    if isinstance(max_age, timedelta):
        max_age = max_age.total_seconds()

    def get_methods():
        if methods is not None:
            return methods

        options_resp = current_app.make_default_options_response()
        return options_resp.headers['allow']

    def decorator(f):
        def wrapped_function(*args, **kwargs):
            if automatic_options and request.method == 'OPTIONS':
                resp = current_app.make_default_options_response()
            else:
                resp = make_response(f(*args, **kwargs))
            if not attach_to_all and request.method != 'OPTIONS':
                return resp

            h = resp.headers

            h['Access-Control-Allow-Origin'] = origin
            h['Access-Control-Allow-Methods'] = get_methods()
            h['Access-Control-Max-Age'] = str(max_age)
            if headers is not None:
                h['Access-Control-Allow-Headers'] = headers
            return resp

        f.provide_automatic_options = False
        return update_wrapper(wrapped_function, f)
    return decorator


def make_cache_key(*args, **kwargs):
    path = request.path
    args = str(hash(frozenset(request.args.items())))
    return (path + args).encode('utf-8')


def make_csv(data):
    outp = StringIO()
    writer = csv.writer(outp)
    writer.writerows(data)
    return outp.getvalue()


def extract_first_geometry_fragment(geojson):
    """
    Given a geojson document, return a geojson geometry fragment marked as 4326 encoding.
    If there are multiple features in the document, just make a fragment of the first feature.
    This is what PostGIS's ST_GeomFromGeoJSON expects.
    :param geojson: A full geojson document
    :type geojson: str
    :return: dict representing geojson structure
    """
    geo = json.loads(geojson)
    if 'features' in geo.keys():
        fragment = geo['features'][0]['geometry']
    elif 'geometry' in geo.keys():
        fragment = geo['geometry']
    else:
        fragment = geo

    return fragment


def make_fragment_str(geojson_fragment, buffer=100):
    if geojson_fragment['type'] == 'LineString':
        shape = asShape(geojson_fragment)
        lat = shape.centroid.y
        x, y = get_size_in_degrees(buffer, lat)
        geojson_fragment = shape.buffer(y).__geo_interface__

    geojson_fragment['crs'] = {"type": "name", "properties": {"name": "EPSG:4326"}}
    return json.dumps(geojson_fragment)