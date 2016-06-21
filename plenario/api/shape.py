import os
import json
import tempfile
import re

from flask import make_response, request

from plenario.models import ShapeMetadata, MetaTable
from plenario.database import session
from plenario.utils.ogr2ogr import OgrExport
from plenario.api.validator import DatasetRequiredValidator, validate
from plenario.api.common import crossdomain, extract_first_geometry_fragment, make_fragment_str, RESPONSE_LIMIT
from plenario.api.point import form_detail_sql_query, form_geojson_detail_response,\
    form_csv_detail_response, bad_request

from collections import OrderedDict
from sqlalchemy import func


def export_dataset_to_response(dataset_name, query=None):

    """
    :param dataset_name: Name of shape dataset. Expected to be found in meta_shape table.
    :param query: Optional SQL query to be executed on shape dataset to filter results
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


def make_sql_ready_geom(geojson_str):
    return make_fragment_str(extract_first_geometry_fragment(geojson_str))    


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

        geom = request.args.get('location_geom__within')
        if geom:
            geom = make_sql_ready_geom(geom)

        public_listing = ShapeMetadata.index(geom)
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
    # Doesn't this override the path-derived parameter with a query parameter?
    # Do we want that?
    if not params.get('shape'):
        # form_detail_query expects to get info about a shape dataset this way.
        params['shape'] = polygon_dataset_name
    params['dataset_name'] = point_dataset_name

    args = validate(DatasetRequiredValidator, params)
    if args.errors:
        return bad_request(args.errors)

    args.data['dataset'] = args.data['dataset'].point_table

    # Apply standard filters to point dataset
    # And join each point to the containing shape
    q = form_detail_sql_query(args, True)
    q = q.add_columns(func.count(args.data['dataset'].c.hash))

    # Apply a bounding box filter in case a geom was provided
    geom = args.data['geom']
    dataset = args.data['dataset']
    if geom:
        intersection = dataset.c.geom.ST_Within(
            func.ST_GeomFromGeoJSON(geom)
        )
        q = q.filter(intersection)

    # Page in RESPONSE_LIMIT chunks
    # This seems contradictory. Don't we want one row per shape, no matter what?
    offset = args.data['offset']
    q = q.limit(RESPONSE_LIMIT)
    if offset > 0:
        q = q.offset(offset)

    res_cols = []
    columns = [str(col) for col in args.data['dataset'].columns]
    columns += [str(col) for col in args.data['shape'].columns]
    for col in columns:
        col = col.split('.')
        if col[0] == polygon_dataset_name:
            res_cols.append(col[1])
    res_cols.append('count')

    rows = [OrderedDict(zip(res_cols, res)) for res in q.all()]
    if params.get('data_type') == 'csv':
        resp = form_csv_detail_response(['hash', 'ogc_fid'], rows)
    else:
        resp = form_geojson_detail_response(['hash', 'ogc_fid'], args, rows)

    return resp


@crossdomain(origin="*")
def export_shape(dataset_name):
    """
    :param dataset_name: Name of shape dataset. Expected to be found in meta_shape table.
    Expected query parameter: `data_type`. We expect it to be one of 'json', 'kml', or 'shapefile'.
                                If none of these (or unspecified), return JSON.
    """

    params = request.args.copy()

    where_string = ""

    if params.get('location_geom__within'):
        fragment = make_fragment_str(extract_first_geometry_fragment(params['location_geom__within']))
        where_string = "WHERE ST_Intersects(g.geom, ST_GeomFromGeoJSON('{}'))".format(fragment)

    query = '''SELECT * FROM {} AS g {}'''.format(dataset_name, where_string)
    return export_dataset_to_response(dataset_name, query)


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