import json
import shapely.wkb
from datetime import datetime
from flask import make_response, request
from plenario.api.common import make_csv, unknown_object_json_handler


def make_error(msg, status_code):
    resp = {
        'meta': {
        },
        'error': msg,
    }

    resp['meta']['query'] = request.args
    return make_response(json.dumps(resp, default=unknown_object_json_handler), status_code)


def bad_request(msg):
    return make_error(msg, 400)


def internal_error(context_msg, exception):
    msg = context_msg + '\nDebug:\n' + repr(exception)
    return make_error(msg, 500)


def json_response_base(data, validator=None, query=''):
    meta = {
        'message': ''
    }

    if validator:
        meta['message'] = validator.warnings
        meta['query'] = query

    return {
        'meta': meta,
        'data': data
    }