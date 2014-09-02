import os
import json
import unittest
from urllib import urlencode
from test_fixtures.test_models import Base, Master, Crime, BusinessLicense
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import csv
from cStringIO import StringIO
from subprocess import call
from sqlalchemy.exc import OperationalError
import plenario.settings

CONN = plenario.settings.DATABASE_CONN

from app import app

def make_rows(fname):
    with open(fname, 'rb') as f:
        reader = csv.DictReader(f)
        for row in reader:
            for k, v in row.items():
                if v == '':
                    row[k] = None
            yield row

class WoprTest(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        call("psql -U postgres -d wopr_test -c \"create extension postgis;\"", shell=True)
        call("psql -U postgres -d wopr_test -c 'grant all on geometry_columns to \"wopr\";'", shell=True)
        call("psql -U postgres -d wopr_test -c \'grant all on spatial_ref_sys to \"wopr\";\'", shell=True)
        self.engine = create_engine(CONN)
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        for row in make_rows('test_fixtures/master.csv'):
            self.session.add(Master(**row))
        for row in make_rows('test_fixtures/crime.csv'):
            self.session.add(Crime(**row))
        for row in make_rows('test_fixtures/business.csv'):
            self.session.add(BusinessLicense(**row))
        self.session.commit()
        self.app = app.test_client()
        self.maxDiff = None
        self.geo = {
            "type":"Feature",
            "properties":{},
            "geometry":{
                "type":"Polygon",
                "coordinates":[
                    [[-87.67913818359375,41.850843709419685],
                      [-87.67913818359375,41.88050217228977],
                      [-87.62969970703125,41.88050217228977],
                      [-87.62969970703125,41.850843709419685],
                      [-87.67913818359375,41.850843709419685]]
                ]
            }
        }

    def test_fields(self):
        fields = self.app.get('/api/fields/chicago_crimes_all/')
        fields = json.loads(fields.data)
        observed_names = [f['field_name'] for f in fields['objects']]
        observed_types = [f['field_type'] for f in fields['objects']]
        expected_names = Crime.__table__.columns.keys()
        expected_types = [unicode(t.type) for t in Crime.__table__.columns]
        self.assertEqual(observed_names, expected_names)
        self.assertEqual(observed_types, expected_types)
    
    def test_fields_no_table(self):
        resp = self.app.get('/api/fields/boogerface/')
        data = json.loads(resp.data)
        expected = {
            'meta': {
                'status': 'error', 
                'message': "'boogerface' is not a valid table name"
              },
            'objects': []
        }
        self.assertEqual(data, expected)
        self.assertEqual(resp.status_code, 400)

    def test_master_table_good_query(self):
        query = {
            'obs_date__ge': '2013/07/15',
            'obs_date__le': '2014/01/01',
            'agg': 'month',
            'geom__within': json.dumps(self.geo),
            'offset': 0,
            'limit': 100,
        }
        resp = self.app.get('/api/master/?%s' % urlencode(query))
        data = json.loads(resp.data)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(data['meta']['status'], 'ok')
        self.assertEqual(len(data['objects']), 2)

    def test_bad_field(self):
        query = {
            'boogers': 'nothing',
        }
        resp = self.app.get('/api/master/?%s' % urlencode(query))
        data = json.loads(resp.data)
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(data['meta']['status'], 'error')
    
    def test_bad_operator(self):
        query = {
            'obs_date__bigger': 'oops',
        }
        resp = self.app.get('/api/master/?%s' % urlencode(query))
        data = json.loads(resp.data)
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(data['meta']['status'], 'error')

    def test_master_csv_output(self):
        query = {
            'obs_date__ge': '2013/07/15',
            'obs_date__le': '2014/01/01',
            'agg': 'month',
            'geom__within': json.dumps(self.geo),
            'offset': 0,
            'limit': 100,
            'dataset_name': 'chicago_crimes_all',
            'datatype': 'csv'
        }
        resp = self.app.get('/api/master/?%s' % urlencode(query))
        observed_first_lines = resp.data.split('\r\n')[:2]
        expected_first_lines = ['count,group,dataset_name', '23,2013-12-01 00:00:00+00:00,chicago_crimes_all']
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(observed_first_lines, expected_first_lines)

    def test_detail_good_query(self):
        query = {
            'obs_date__ge': '2013/07/15',
            'obs_date__le': '2014/01/01',
            'agg': 'day',
            'geom__within': json.dumps(self.geo),
            'offset': 0,
            'limit': 100,
            'fbi_code__in': '10,11,12,13,14,15,16,17,18,19,20,22,24,26',
            'dataset_name': 'chicago_crimes_all',
        }
        path = '/api/detail-aggregate/?%s' % urlencode(query)
        resp = self.app.get(path)
        data = json.loads(resp.data)
        items = data['objects'][0]['items']
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(items), 1)
    
    def test_detail_csv_output(self):
        query = {
            'obs_date__ge': '2013/07/15',
            'obs_date__le': '2014/01/01',
            'agg': 'day',
            'geom__within': json.dumps(self.geo),
            'offset': 0,
            'limit': 100,
            'fbi_code__in': '10,11,12,13,14,15,16,17,18,19,20,22,24,26',
            'dataset_name': 'chicago_crimes_all',
            'datatype': 'csv'
        }
        path = '/api/detail/?%s' % urlencode(query)
        resp = self.app.get(path)
        data = StringIO(resp.data)
        reader = csv.DictReader(data)
        rows = [r for r in reader]
        fields = rows[0].keys()
        cases = [c['case_number'] for c in rows]
        self.assertIn('case_number', fields)
        self.assertIn('block', fields)
        self.assertIn('iucr', fields)
        self.assertIn('HW569534', cases)
        self.assertEqual(resp.status_code, 200)


    def test_default_api(self):
        resp = self.app.get('/api/')
        data = json.loads(resp.data)
        self.assertEqual(resp.status_code, 200)
        observed_names = [r['machine_name'] for r in data]
        expected_names = ['chicago_business_licenses', 'chicago_crimes_all']
        self.assertEqual(observed_names, expected_names)

    @classmethod
    def tearDownClass(self):
        Base.metadata.drop_all(self.engine)
        self.session.close()

if __name__ == "__main__":
    unittest.main()
