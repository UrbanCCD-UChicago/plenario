import json
from flask import make_response, request


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
