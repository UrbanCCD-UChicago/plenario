import json


from collections import OrderedDict
from flask import make_response, request
from sqlalchemy import func
from sqlalchemy.exc import NoSuchTableError

from plenario.api.common import crossdomain, extract_first_geometry_fragment
from plenario.api.common import make_fragment_str
from plenario.api.condition_builder import parse_tree
from plenario.api.point import detail_query
from plenario.api.jobs import make_job_response
from plenario.api.response import export_dataset_to_response, make_error
from plenario.api.response import aggregate_point_data_response, bad_request
from plenario.api.validator import validate, has_tree_filters, Validator
from plenario.api.validator import ExportFormatsValidator
from plenario.models import ShapeMetadata


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
            geom = make_fragment_str(
                extract_first_geometry_fragment(geom)
            )

        public_listing = ShapeMetadata.index(geom)
        response_skeleton['objects'] = public_listing
        status_code = 200

    except Exception as e:
        print(e)
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
    consider = ('dataset_name', 'shape', 'obs_date__ge', 'obs_date__le',
                'data_type', 'location_geom__within', 'job')

    request_args = request.args.to_dict()
    request_args['dataset_name'] = point_dataset_name
    request_args['shape'] = polygon_dataset_name

    validated_args = validate(Validator(only=consider), request_args)

    if validated_args.errors:
        return bad_request(validated_args.errors)
    elif validated_args.data.get('job'):
        return make_job_response('aggregate-point-data', validated_args)
    else:
        result = _aggregate_point_data(validated_args)
        data_type = validated_args.data.get('data_type')
        return aggregate_point_data_response(
            data_type,
            result,
            [polygon_dataset_name, point_dataset_name]
        )


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

    meta_params = ('shape', 'data_type', 'location_geom__within', 'job')
    request_args = request.args.to_dict()

    # Using the 'shape' key triggers the correct validator.
    request_args['shape'] = dataset_name
    validated_args = validate(
        ExportFormatsValidator(only=meta_params),
        request_args
    )

    if validated_args.errors:
        return bad_request(validated_args.errors)
    elif validated_args.data.get('job'):
        return make_job_response('export-shape', validated_args)
    else:
        query = _export_shape(validated_args)
        shapeset = validated_args.data.get('shapeset')
        data_type = validated_args.data.get('data_type')
        return export_dataset_to_response(shapeset, data_type, query)


# =================
# Shape Route Logic
# =================

def _aggregate_point_data(args):
    meta_params = ('dataset', 'shapeset', 'data_type',
                   'geom', 'offset', 'limit')
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

    return [OrderedDict(list(zip(res_cols, res))) for res in q.all()]


def _export_shape(args):
    """Route logic for /shapes/<shapeset>/ endpoint. Returns records for a
    single specified shape dataset.

    :param args: ValidatorResult of user provided arguments

    :returns: response object"""

    meta_params = ('shapeset', 'data_type', 'geom')
    meta_vals = (args.data.get(k) for k in meta_params)
    shapeset, data_type, geom = meta_vals

    if shapeset is None:
        error_message = 'Could not find shape dataset {}'
        error_message = error_message.format(request.args['shape'])
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

    return query
