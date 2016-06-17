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

print len(json.loads(test.get('/v1/api/datasets?dataset_name=crimes').data)['objects'])
print len(json.loads(test.get('/v1/api/datasets/?dataset_name=crimes').data)['objects'])


print "\n"
print "========================================"
print "\n"
