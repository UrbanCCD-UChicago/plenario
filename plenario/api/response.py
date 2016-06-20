import json
import shapely.wkb
from datetime import datetime
from flask import make_response, request
from plenario.api.common import dthandler, make_csv, unknownObjectHandler


def make_error(msg, status_code):
    resp = {
        'meta': {
            'status': 'error',
            'message': msg,
        },
        'objects': [],
    }

    resp['meta']['query'] = request.args
    return make_response(json.dumps(resp), status_code)


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
    resp['meta']['query'] = validator.data
    resp = make_response(
        json.dumps(resp, default=unknownObjectHandler),
        200
    )
    resp.headers['Content-Type'] = 'application/json'
    return resp


def form_csv_detail_response(to_remove, validator, rows):
    to_remove.append('geom')
    remove_columns_from_dict(rows, to_remove)

    # Column headers from arbitrary row,
    # then the values from all the others
    csv_resp = [rows[0].keys()] + [row.values() for row in rows]
    resp = make_response(make_csv(csv_resp), 200)
    dname = validator.dataset.name  # dataset_name
    filedate = datetime.now().strftime('%Y-%m-%d')
    resp.headers['Content-Type'] = 'text/csv'
    resp.headers['Content-Disposition'] = 'attachment; filename=%s_%s.csv' % (dname, filedate)
    return resp


def form_geojson_detail_response(to_remove, validator, rows):
    geojson_resp = geojson_response_base()
    # We want the geom this time.
    remove_columns_from_dict(rows, to_remove)

    for row in rows:
        try:
            wkb = row.pop('geom')
            geom = shapely.wkb.loads(wkb.desc, hex=True).__geo_interface__
        except (KeyError, AttributeError):
            # If we couldn't fund a geom value,
            # or said value was not of the expected type,
            # then skip this column
            continue
        else:
            add_geojson_feature(geojson_resp, geom, row)

    resp = make_response(json.dumps(geojson_resp, default=dthandler), 200)
    resp.headers['Content-Type'] = 'application/json'
    return resp
