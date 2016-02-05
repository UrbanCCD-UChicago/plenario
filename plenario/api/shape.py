import os
import json
import tempfile

from flask import make_response, request

from plenario.models import ShapeMetadata
from plenario.database import session, app_engine as engine
from plenario.utils.ogr2ogr import OgrExport
from plenario.api.common import crossdomain, extract_first_geometry_fragment, make_fragment_str, RESPONSE_LIMIT
from plenario.api.point import ParamValidator, setup_detail_validator, form_detail_sql_query, form_geojson_detail_response, bad_request

from collections import OrderedDict
from sqlalchemy import func

def export_dataset_to_json_response(dataset_name, query=None):

    """
    :param dataset_name: Name of shape dataset. Expected to be found in meta_shape table.
    :param query: Optional SQL query to be executed on shape dataset to filer results
    Expected query parameter: `data_type`. We expect it to be one of 'json', 'kml', or 'shapefile'.
                                If none of these (or unspecified), return JSON.
    :return: response with geoJSON data and response code
    """

    # Do we have this shape?
    shape_dataset = session.query(ShapeMetadata).get(dataset_name)
    if not (shape_dataset and shape_dataset.is_ingested):
        error_message = 'Could not find shape dataset {}'.format(dataset_name)
        return make_response(error_message, 404)

    # What file format does the user want it in?
    export_format = request.args.get('data_type')
    # json is default export type
    if not export_format:
        export_format = u'json'
    export_format = unicode.lower(export_format)

    # Make a filename that we are reasonably sure to be unique and not occupied by anyone else.
    sacrifice_file = tempfile.NamedTemporaryFile()
    export_path = sacrifice_file.name
    sacrifice_file.close()  # Removes file from system.

    try:
        # Write to that filename
        OgrExport(export_format=export_format, table_name=dataset_name, export_path=export_path, query=query).write_file()
        # Dump it in the response
        with open(export_path, 'r') as to_export:
            resp = make_response(to_export.read(), 200)

        # Make the downloaded filename look nice
        resp.headers['Content-Type'] = _shape_format_to_content_header(export_format)
        disp_header = 'attachment; filename={name}.{ext}'.format(name=shape_dataset.human_name,
                                                                 ext=_shape_format_to_file_extension(export_format))
        resp.headers['Content-Disposition'] = disp_header
        return resp
    except Exception as e:
        error_message = 'Failed to export shape dataset {}'.format(dataset_name)
        print repr(e)
        return make_response(error_message, 500)
    finally:
        # Don't leave that file hanging around.
        if os.path.isfile(export_path):
            os.remove(export_path)

@crossdomain(origin="*")
def get_all_shape_datasets():
    """
    Fetches metadata for every shape dataset in meta_shape
    """
    try:
        response_skeleton = {
                'meta': {
                    'status': 'ok',
                    'message': '',
                },
                'objects': []
            }

        public_listing = ShapeMetadata.index()
        response_skeleton['objects'] = public_listing
        status_code = 200

    except Exception as e:
        print e.message
        response_skeleton = {
            'meta': {
                'status': 'error',
                'message': '',
            }
        }
        status_code = 500

    resp = make_response(json.dumps(response_skeleton), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp

def aggregate_point_data(point_dataset_name, polygon_dataset_name):

    params = request.args.copy()
    if not params.get('shape'):
        params['shape'] = polygon_dataset_name

    validator = setup_detail_validator(point_dataset_name, params)

    err = validator.validate(params)
    if err:
        return bad_request(err)

    q = form_detail_sql_query(validator, True)
    q = q.add_columns(func.count(validator.dataset.c.id))

    # Page in RESPONSE_LIMIT chunks
    offset = validator.vals['offset']
    q = q.limit(RESPONSE_LIMIT)
    if offset > 0:
        q = q.offset(offset)

    res_cols = []
    for col in validator.cols:
        if col[:len(polygon_dataset_name)] == polygon_dataset_name:
            res_cols.append(col[len(polygon_dataset_name)+1:])
    res_cols.append('count')

    rows = [OrderedDict(zip(res_cols, res)) for res in q.all()]
    resp = form_geojson_detail_response([], validator, rows)

    return resp

@crossdomain(origin="*")
def filter_shape(dataset_name, geojson):
    """
    Given a shape dataset and user-provided geojson,
    return all shapes from the dataset that intersect the geojson.

    :param dataset_name: Name of shape dataset
    :param geojson: URL encoded goejson
    :return:
    """
    fragment = make_fragment_str(extract_first_geometry_fragment(geojson))

    intersect_query = '''
    SELECT *
    FROM {dataset_name} AS g
    WHERE ST_Intersects(g.geom, ST_GeomFromGeoJSON('{geojson_fragment}'))
    '''.format(dataset_name=dataset_name, geojson_fragment=fragment)

    return export_dataset_to_json_response(dataset_name, intersect_query)

@crossdomain(origin="*")
def find_intersecting_shapes(geojson):
    """
    Respond with all shape datasets that intersect with the geojson provided.
    Also include how many geom rows of the dataset intersect.
    :param geojson: URL encoded geojson.
    """
    fragment = make_fragment_str(extract_first_geometry_fragment(geojson))

    try:
        # First, do a bounding box check on all shape datasets.
        query = '''
        SELECT m.dataset_name
        FROM meta_shape as m
        WHERE m.bbox &&
            (SELECT ST_GeomFromGeoJSON('{geojson_fragment}'));
        '''.format(geojson_fragment=fragment)

        intersecting_datasets = engine.execute(query)
        bounding_box_intersection_names = [dataset.dataset_name for dataset in intersecting_datasets]
    except Exception as e:
        print 'Error finding candidates'
        raise e

    try:
        # Then, for the candidates, get a count of the rows that intersect.
        response_objects = []
        for dataset_name in bounding_box_intersection_names:
            num_intersections_query = '''
            SELECT count(g.geom) as num_geoms
            FROM {dataset_name} as g
            WHERE ST_Intersects(g.geom, ST_GeomFromGeoJSON('{geojson_fragment}'))
            '''.format(dataset_name=dataset_name, geojson_fragment=fragment)

            num_intersections = engine.execute(num_intersections_query)\
                                      .first().num_geoms

            if num_intersections > 0:
                response_objects.append({
                    'dataset_name': dataset_name,
                    'num_geoms': num_intersections
                })

    except Exception as e:
        print 'Error narrowing candidates'
        raise e

    response_skeleton = {
                'meta': {
                    'status': 'ok',
                    'message': '',
                },
                'objects': response_objects
            }

    resp = make_response(json.dumps(response_skeleton), 200)
    return resp

@crossdomain(origin="*")
def export_shape(dataset_name):
    """
    :param dataset_name: Name of shape dataset. Expected to be found in meta_shape table.
    Expected query parameter: `data_type`. We expect it to be one of 'json', 'kml', or 'shapefile'.
                                If none of these (or unspecified), return JSON.
    """
    
    return export_dataset_to_json_response(dataset_name)

def _shape_format_to_content_header(requested_format):

    format_map = {
        'json': 'application/json',
        'kml':  'application/vnd.google-earth.kml+xml',
        'shapefile': 'application/zip'
    }
    return format_map[requested_format]


def _shape_format_to_file_extension(requested_format):
    format_map = {
        'json': 'json',
        'kml': 'kml',
        'shapefile': 'zip'
    }
    return format_map[requested_format]