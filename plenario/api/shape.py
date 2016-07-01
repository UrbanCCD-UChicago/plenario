import os
import json
import tempfile

from collections import OrderedDict
from flask import make_response, request
from sqlalchemy import func
from sqlalchemy.exc import NoSuchTableError

from plenario.api.common import crossdomain, extract_first_geometry_fragment
from plenario.api.common import make_fragment_str
from plenario.api.condition_builder import parse_tree
from plenario.api.point import detail_query, form_csv_detail_response
from plenario.api.point import form_geojson_detail_response, bad_request
from plenario.api.response import make_error
from plenario.api.validator import validate, has_tree_filters, Validator
from plenario.models import ShapeMetadata
from plenario.utils.ogr2ogr import OgrExport


# ====================
# Shape Format Headers
# ====================

def _shape_format_to_content_header(requested_format):
    format_map = {
        'json': 'application/json',
        'kml': 'application/vnd.google-earth.kml+xml',
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


# ============
# Shape Routes
# ============

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
            make_fragment_str(
                extract_first_geometry_fragment(geom)
            )

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


@crossdomain(origin="*")
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

    meta_params = ('shapeset_name', 'data_type', 'location_geom__within')
    request_args = request.args.to_dict()

    # Using the 'shapeset_name' key triggers the correct validator.
    request_args['shapeset_name'] = dataset_name
    validated_args = validate(Validator(only=meta_params), request_args)
    if validated_args.errors:
        return bad_request(validated_args.errors)
    return _export_shape(validated_args)


# =================
# Shape Route Logic
# =================

def _aggregate_point_data(args):
    meta_params = ('dataset', 'shapeset', 'data_type', 'geom', 'offset', 'limit')
    meta_vals = (args.data.get(k) for k in meta_params)
    dataset, shapeset, data_type, geom, offset, limit = meta_vals

    q = detail_query(args, aggregate=True)
    q = q.add_columns(func.count(dataset.c.hash))

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


def _export_shape(args):
    """Route logic for /shapes/<shapeset>/ endpoint. Returns records for a
    single specified shape dataset.

    :param args: ValidatorResult of user provided arguments

    :returns: response object"""

    meta_params = ('shapeset', 'data_type', 'geom')
    meta_vals = (args.data.get(k) for k in meta_params)
    shapeset, data_type, geom = meta_vals

    # TODO: This is the validator's job.
    if shapeset is None:
        error_message = 'Could not find shape dataset {}'.format(request.args['shapeset_name'])
        return make_response(error_message, 404)

    query = "SELECT * FROM {}".format(shapeset.name)
    conditions = ""

    if has_tree_filters(args.data):
        # A string literal is required for ogr2ogr to function correctly.
        ctree = args.data[shapeset.name + '__filter']
        conditions = str(parse_tree(shapeset, ctree, literally=True))

    if geom:
        if conditions:
            conditions += "AND "
        conditions += "ST_Intersects({}.geom, ST_GeomFromGeoJSON('{}'))".format(
            shapeset.name, geom)

    if conditions:
        query += " WHERE " + conditions

    return _export_dataset_to_response(shapeset, data_type, query)


def _export_dataset_to_response(shapeset, data_type, query=None):
    export_format = unicode.lower(unicode(data_type))

    # Make a filename that we are reasonably sure to be unique and not occupied by anyone else.
    sacrifice_file = tempfile.NamedTemporaryFile()
    export_path = sacrifice_file.name
    sacrifice_file.close()  # Removes file from system.

    try:
        # Write to that filename.
        OgrExport(export_format, export_path, shapeset.name, query).write_file()
        # Dump it in the response.
        with open(export_path, 'r') as to_export:
            resp = make_response(to_export.read(), 200)

        extension = _shape_format_to_file_extension(export_format)

        # Make the downloaded filename look nice
        shapemeta = ShapeMetadata.get_by_dataset_name(shapeset.name)
        resp.headers['Content-Type'] = _shape_format_to_content_header(export_format)
        resp.headers['Content-Disposition'] = 'attachment; filename={}.{}'.format(shapemeta.human_name, extension)
        return resp

    except Exception as e:
        error_message = 'Failed to export shape dataset {}'.format(shapeset.name)
        print repr(e)
        return make_response(error_message, 500)
    finally:
        # Don't leave that file hanging around.
        if os.path.isfile(export_path):
            os.remove(export_path)
