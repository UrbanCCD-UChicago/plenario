import json
import os
import sys
sys.path.insert(0, os.path.abspath('.'))

from flask import request
from plenario import create_app
from plenario.api.point import _meta, _fields, _grid


def get_my_request(query):

    t = test.get(query)
    return json.loads(t.data)

print "\n"
print "========================================"
print "\n"

app = create_app()
test = create_app().test_client()

with app.test_request_context('/v1/api/grid?dataset_name=crimes'):
    print request.args
    print _grid(request.args.to_dict())

print "\n"
print "========================================"
print "\n"
