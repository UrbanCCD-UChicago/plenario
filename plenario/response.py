from flask import abort, jsonify, Response
from webargs.core import ValidationError
from webargs.flaskparser import FlaskParser

from plenario.server import db


parser = FlaskParser()


@parser.error_handler
def handler(error: ValidationError):
    """Overwrite the error handler for a webargs argument parser in order to
    return a json-api-ish response on invalid query arguments."""

    body = {'meta': {
        'status': error.status_code,
        'query': error.data
    }, 'errors': error.messages}
    response = jsonify(body)
    response.status_code = error.status_code
    db.session.rollback()
    abort(response)


class JsonResponse(Response):
    """This response class enables can optionally receive a dictionary to
    return a response with the json content type. If the value is not a
    dictionary it behaves as a normal response."""

    @classmethod
    def force_type(cls, rv, environ=None):
        if isinstance(rv, dict):
            rv = jsonify(rv)
        return super(JsonResponse, cls).force_type(rv, environ)


def ok(args, results):
    """Returns a dictionary payload that is common to all of the point api."""

    return {'meta': {
        'status': 200,
        'query': args,
        'total': len(results)
    }, 'objects': results}
