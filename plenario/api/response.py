import json
import os
import shapely.wkb
import tempfile

from plenario.api.common import make_csv, date_json_handler
from plenario.api.common import unknown_object_json_handler
from plenario.models import ShapeMetadata
from plenario.utils.ogr2ogr import OgrExport

from datetime import datetime
from flask import make_response, request
from itertools import groupby
from operator import itemgetter


def make_error(msg, status_code):
    resp = {
        'meta': {
            'status': 'error',
            'message': msg,
        },
        'objects': [],
    }

    resp['meta']['query'] = request.args
    return make_response(json.dumps(resp, default=unknown_object_json_handler), status_code)


def make_raw_error(msg):
    resp = {
        'meta': {
            'status': 'error',
            'message': msg,
        },
        'objects': [],
    }
    return resp


def bad_request(msg):
    return make_error(msg, 400)


def internal_error(context_msg, exception):
    msg = context_msg + '\nDebug:\n' + repr(exception)
    return make_error(msg, 500)


def remove_columns_from_dict(rows, col_names):
    for row in rows:
        for name in col_names:
            try:
                del row[name]
            except KeyError:
                pass


def json_response_base(validator, objects, query=''):
    meta = {
        'status': 'ok',
        'message': '',
        'query': query,
    }

    if validator:
        meta['message'] = validator.warnings
        meta['query'] = query

    return {
        'meta': meta,
        'objects': objects,
    }


def geojson_response_base():
    return {
        "type": "FeatureCollection",
        "features": []
    }


def add_geojson_feature(geojson_response, feature_geom, feature_properties):
    new_feature = {
        "type": "Feature",
        "geometry": feature_geom,
        "properties": feature_properties
    }
    geojson_response['features'].append(new_feature)


def form_json_detail_response(to_remove, validator, rows):
    to_remove.append('geom')
    remove_columns_from_dict(rows, to_remove)
    resp = json_response_base(validator, rows)
    resp['meta']['total'] = len(resp['objects'])
    resp['meta']['query'] = request.args
    resp = make_response(
        json.dumps(resp, default=unknown_object_json_handler),
        200
    )
    resp.headers['Content-Type'] = 'application/json'
    return resp


def form_csv_detail_response(to_remove, rows):
    to_remove.append('geom')
    print "rows ->> " + str(rows)
    remove_columns_from_dict(rows, to_remove)

    # Column headers from arbitrary row,
    # then the values from all the others
    csv_resp = [rows[0].keys()] + [row.values() for row in rows]
    resp = make_response(make_csv(csv_resp), 200)
    dname = request.args.get('dataset_name')
    filedate = datetime.now().strftime('%Y-%m-%d')
    resp.headers['Content-Type'] = 'text/csv'
    resp.headers['Content-Disposition'] = 'attachment; filename=%s_%s.csv' % (dname, filedate)
    return resp


def form_geojson_detail_response(to_remove, rows):
    remove_columns_from_dict(rows, to_remove)
    geojson_resp = convert_result_geoms(rows)
    resp = make_response(json.dumps(geojson_resp, default=unknown_object_json_handler), 200)
    resp.headers['Content-Type'] = 'application/json'
    return resp


def convert_result_geoms(result):
    """Given a list of rows, convert the geom for each row from a shape
    to a list of coordinates.

    :param result: (list) contains the results of some query
    :returns (list) modified result, where geoms are represented by lists"""

    geojson_resp = geojson_response_base()
    for row in result:
        try:
            wkb = row.pop('geom')
            geom = shapely.wkb.loads(wkb.desc, hex=True).__geo_interface__
        except (KeyError, AttributeError):
            continue
        else:
            add_geojson_feature(geojson_resp, geom, row)
    return geojson_resp


# Point Endpoint Repsonses ====================================================

def detail_aggregate_response(query_result, query_args):

    datatype = query_args.data['data_type']

    if datatype == 'csv':
        resp = make_csv(query_result)
        resp.headers['Content-Type'] = 'text/csv'
        filedate = datetime.now().strftime('%Y-%m-%d')
        resp.headers['Content-Disposition'] = 'attachment; filename=%s.csv' % filedate
    else:
        resp = json_response_base(query_args, query_result, request.args)
        resp['count'] = sum([c['count'] for c in query_result])
        resp = make_response(json.dumps(resp, default=unknown_object_json_handler), 200)
        resp.headers['Content-Type'] = 'application/json'

    return resp


def meta_response(query_result, query_args):

    resp = json_response_base(query_args, query_result, request.args)
    resp['meta']['total'] = len(resp['objects'])
    status_code = 200
    resp = make_response(json.dumps(resp, default=unknown_object_json_handler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp


def fields_response(query_result, query_args):

    resp = json_response_base(query_args, query_result, request.args)
    resp['objects'] = query_result[0]['columns']
    status_code = 200
    resp = make_response(json.dumps(resp, default=unknown_object_json_handler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp


def grid_response(query_result):

    resp = make_response(json.dumps(query_result, default=date_json_handler), 200)
    resp.headers['Content-Type'] = 'application/json'
    return resp


def detail_response(query_result, query_args):

    to_remove = ['point_date', 'hash']

    data_type = query_args.data['data_type']
    if data_type == 'json':
        return form_json_detail_response(to_remove, query_args, query_result)

    elif data_type == 'csv':
        return form_csv_detail_response(to_remove, query_result)

    elif data_type == 'geojson':
        return form_geojson_detail_response(to_remove, query_result)


def timeseries_response(query_result, query_args):

    resp = json_response_base(query_args, query_result, query_args.data)

    datatype = query_args.data['data_type']
    if datatype == 'json':
        resp = make_response(json.dumps(resp, default=unknown_object_json_handler), 200)
        resp.headers['Content-Type'] = 'application/json'
    elif datatype == 'csv':

        # response format
        # temporal_group,dataset_name_1,dataset_name_2
        # 2014-02-24 00:00:00,235,653
        # 2014-03-03 00:00:00,156,624

        fields = ['temporal_group']
        for o in resp['objects']:
            fields.append(o['dataset_name'])

        csv_resp = []
        i = 0
        for k, g in groupby(resp['objects'], key=itemgetter('dataset_name')):
            l_g = list(g)[0]

            j = 0
            for row in l_g['items']:
                # first iteration, populate the first column with temporal_groups
                if i == 0:
                    csv_resp.append([row['datetime']])
                csv_resp[j].append(row['count'])
                j += 1
            i += 1

        csv_resp.insert(0, fields)
        csv_resp = make_csv(csv_resp)
        resp = make_response(csv_resp, 200)
        resp.headers['Content-Type'] = 'text/csv'
        filedate = datetime.now().strftime('%Y-%m-%d')
        resp.headers['Content-Disposition'] = 'attachment; filename=%s.csv' % filedate

    return resp


# Shape Endpoint Responses ====================================================

def aggregate_point_data_response(data_type, rows):
    if data_type == 'csv':
        return form_csv_detail_response(['hash', 'ogc_fid'], rows)
    else:
        return form_geojson_detail_response(['hash', 'ogc_fid'], rows)


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


def export_dataset_to_response(shapeset, data_type, query=None):
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
