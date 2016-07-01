import csv
import os
import json
import tempfile
import re

from collections import OrderedDict
from flask import make_response, request
from sqlalchemy import func, and_
from sqlalchemy.exc import NoSuchTableError

from plenario.api.common import crossdomain, extract_first_geometry_fragment
from plenario.api.common import make_fragment_str, RESPONSE_LIMIT
from plenario.api.condition_builder import parse_tree
from plenario.api.point import _detail_query, form_csv_detail_response
from plenario.api.point import form_geojson_detail_response, bad_request, request_args_to_condition_tree
from plenario.api.response import make_error
from plenario.api.validator import DatasetRequiredValidator, validate, has_tree_filters
from plenario.api.validator import Validator
from plenario.database import session
from plenario.models import ShapeMetadata, MetaTable
from plenario.utils.ogr2ogr import OgrExport


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
        OgrExport(export_format=export_format, table_name=dataset_name, export_path=export_path, query=query).write_file()        # Dump it in the response
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

    consider = ('dataset_name', 'shapeset_name', 'obs_date__ge', 'obs_date__le',
                'data_type', 'location_geom__within')

    request_args = request.args.to_dict()
    request_args['dataset_name'] = point_dataset_name
    request_args['shapeset_name'] = polygon_dataset_name

    validated_args = validate(Validator(only=consider), request_args)

    if validated_args.errors:
        return bad_request(validated_args.errors)

    return _aggregate_point_data(validated_args)


def _aggregate_point_data(args):

    # TODO: Perform _detail's query.
    # TODO: Format response.
    # TODO: Output response.

    meta_params = ('dataset', 'shapeset', 'data_type', 'geom', 'offset', 'limit')
    meta_vals = (args.data.get(k) for k in meta_params)
    dataset, shapeset, data_type, geom, offset, limit = meta_vals

    q = _detail_query(args, aggregate=True)
    q = q.add_columns(func.count(dataset.c.hash))

    # TODO: Take a good hard look at this.
    res_cols = []
    columns = [str(col) for col in dataset.columns]
    columns += [str(col) for col in shapeset.columns]
    for col in columns:
        col = col.split('.')
        if col[0] == shapeset.name:
            res_cols.append(col[1])
    res_cols.append('count')

    rows = [OrderedDict(zip(res_cols, res)) for res in q.all()]
    if data_type == 'csv':
        return form_csv_detail_response(['hash', 'ogc_fid'], rows)
    else:
        return form_geojson_detail_response(['hash', 'ogc_fid'], args, rows)


@crossdomain(origin="*")
def export_shape(dataset_name):
    """Route for /shapes/<shapeset>/ endpoint. Requires a dataset argument
    and can apply column specific filters to it.

    :param dataset_name: user provided name of target shapeset

    :returns: response object result of _export_shape"""

    # Find a way to work these into the validator, they shouldn't be out here.
    if dataset_name not in ShapeMetadata.tablenames():
        return make_error(dataset_name + ' not found.', 404)
    try:
        ShapeMetadata.get_by_dataset_name(dataset_name).shape_table
    except NoSuchTableError:
        return make_error(dataset_name + ' has yet to be ingested.', 404)

    request_args = request.args.to_dict()
    # Using the 'shape' key triggers the correct converter.
    request_args['shape'] = dataset_name
    validated_args = validate(Validator(only=request_args.keys()), request_args)
    if validated_args.errors:
        return bad_request(validated_args.errors)
    return _export_shape(validated_args)


def _export_shape(request_):
    """Route logic for /shapes/<shapeset>/ endpoint. Returns records for a
    single specified shape dataset.

    :param request_: ValidatorResult of user provided arguments

    :returns: response object"""

    request_args = request_.data
    shapeset = request_args.get('shape')
    geojson = request_args.get('geom')

    query = "SELECT * FROM {}".format(shapeset.name)
    conditions = ""

    if has_tree_filters(request_args):
        # A string literal is required for ogr2ogr to function correctly.
        ctree = request_args[shapeset.name + '__filter']
        conditions = str(parse_tree(shapeset, ctree, literally=True))

    if geojson:
        if conditions:
            conditions += "AND "
        conditions += "ST_Intersects({}.geom, ST_GeomFromGeoJSON('{}'))".format(
            shapeset.name, geojson)

    if conditions:
        query += " WHERE " + conditions

    return export_dataset_to_response(shapeset.name, query)


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