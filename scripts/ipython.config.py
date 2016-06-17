import json
import os
import sys
sys.path.insert(0, os.path.abspath('.'))

from flask import request
from plenario import create_app
from plenario.api.point import _meta, _fields

print "\n"
print "========================================"
print "\n"

app = create_app()
test = create_app().test_client()

with app.test_request_context('/v1/api/datasets?dataset_name=crimes'):

    request_args = request.args.to_dict()
    datasets_response = _meta(request_args)
    datasets = json.loads(datasets_response.data)

fields_response = test.get('/v1/api/fields/crimes')
fields = json.loads(fields_response.data)

print fields == datasets

print "\n"
print "========================================"
print "\n"
