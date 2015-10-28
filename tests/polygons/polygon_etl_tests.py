import unittest
import os
from hashlib import md5
from plenario.utils.polygon_etl import PolygonETL
from plenario.utils.etl import PlenarioETL
from plenario.utils.shapefile import Shapefile
from plenario import create_app
from init_db import init_master_meta_user, init_census
from plenario.database import session, app_engine as engine
from plenario.models import MetaTable, PolygonMetadata
import json
import urllib
from StringIO import StringIO
import zipfile

pwd = os.path.dirname(os.path.realpath(__file__))
FIXTURE_PATH = os.path.join(pwd, '..', 'test_fixtures')

# The shapefiles I'm using for testing are projected as NAD_1983_StatePlane_Illinois_East_FIPS_1201_Feet
ILLINOIS_STATE_PLANE_SRID = 3435


def drop_tables(table_names):
    drop_template = 'DROP TABLE IF EXISTS {};'
    command = ''.join([drop_template.format(table_name) for table_name in table_names])
    engine.execute(command)


class Fixture(object):
    def __init__(self, human_name, file_name):
        self.human_name = human_name
        self.table_name = PolygonMetadata.make_table_name(human_name)
        self.path = os.path.join(FIXTURE_PATH, file_name)

fixtures = {
    'city': Fixture(human_name=u'Chicago City Limits',
                    file_name='chicago_city_limits.zip'),
    'streets': Fixture(human_name=u'Pedestrian Streets',
                       file_name='chicago_pedestrian_streets.zip'),
    'zips': Fixture(human_name=u'Zip Codes',
                    file_name='chicago_zip_codes.zip')
}


class PolygonAPITests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):

        # Remove tables that we're about to recreate.
        # This doesn't happen in teardown because I find it helpful to inspect them in the DB after running the tests.
        meta_table_names = ['dat_master', 'meta_polygon', 'meta_master', 'plenario_user']
        fixture_table_names = [fixture.table_name for key, fixture in fixtures.iteritems()]
        drop_tables(meta_table_names + fixture_table_names)

        # Re-add meta tables
        init_master_meta_user()

        cls.city_meta = PolygonAPITests.ingest_fixture(fixtures['city'])
        cls.streets_meta = PolygonAPITests.ingest_fixture(fixtures['streets'])
        cls.zips_meta = PolygonAPITests.ingest_fixture(fixtures['zips'])
        session.commit()

        cls.app = create_app().test_client()

    @staticmethod
    def ingest_fixture(fixture):
            shape_meta = PolygonMetadata.add(caller_session=session, human_name=fixture.human_name, source_url=None)
            session.commit()
            PolygonETL(meta=shape_meta, source_path=fixture.path).import_shapefile()
            return shape_meta

    def test_names_in_polygon_list(self):
        resp = self.app.get('/v1/api/polygons/')
        response_data = json.loads(resp.data)
        all_names = [item['dataset_name'] for item in response_data['objects']]
        self.assertIn(fixtures['streets'].table_name, all_names)
        self.assertIn(fixtures['zips'].table_name, all_names)

    def test_find_intersecting(self):
        rect_path = os.path.join(FIXTURE_PATH, 'university_village_rectangle.json')
        with open(rect_path, 'r') as rect_json:
            query_rect = rect_json.read()
        escaped_query_rect = urllib.quote(query_rect)

        resp = self.app.get('/v1/api/polygons/intersections/' + escaped_query_rect)
        self.assertEqual(resp.status_code, 200)
        response_data = json.loads(resp.data)

        # By design, the query rectangle should cross 3 zip codes and 2 pedestrian streets
        datasets_to_num_geoms = {obj['dataset_name']: obj['num_geoms'] for obj in response_data['objects']}
        self.assertEqual(datasets_to_num_geoms[fixtures['zips'].table_name], 3)
        self.assertEqual(datasets_to_num_geoms[fixtures['streets'].table_name], 2)

    def test_export_geojson(self):
        # Do we at least get some json back?
        resp = self.app.get('/v1/api/polygons/{}?data_type=json'.format(fixtures['city'].table_name))
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.content_type, 'application/json')

        # Dropping the fixture shapefile into QGIS shows that there are 13,403 points in the multipolygon.
        expected_num_points = 13403

        # Do we get 13,403 points back?
        city_geojson = json.loads(resp.data)
        city_limits = city_geojson['features'][0]['geometry']['coordinates']
        observed_num_points = 0
        for outer_geom in city_limits:
            for inner_geom in outer_geom:
                observed_num_points += len(inner_geom)
        self.assertEqual(expected_num_points, observed_num_points)

    def test_export_with_bad_name(self):
        resp = self.app.get('/v1/api/polygons/this_is_a_fake_name')
        self.assertEqual(resp.status_code, 404)

    def test_export_shapefile(self):
        resp = self.app.get('/v1/api/polygons/{}?data_type=shapefile'.format(fixtures['city'].table_name))
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.content_type, 'application/zip')
        file_content = StringIO(resp.data)
        as_zip = zipfile.ZipFile(file_content)

        # The Shapefile utility class takes a ZipFile, opens it,
        # and throws an exception if it doesn't have the expected shapefile components (.shp and ,prj namely)
        with Shapefile(as_zip):
            pass

    def test_no_import_when_name_conflict(self):
        with self.assertRaises(Exception):
            PolygonAPITests.ingest_fixture(fixtures['city'])

    # TODO: test case for exporting table that's in metadata but not ingested

    # TODO: test cases for alternative export types


class CensusRegressionTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Assume there exists a test database with postgis at the connection string specified in test_settings.py
        tables_to_drop = ['census_blocks',
                          'dat_flu_shot_clinic_locations',
                          'dat_master',
                          'meta_master',
                          'meta_polygon',
                          'plenario_user']
        drop_tables(tables_to_drop)

        # Create meta, master, user tables
        init_master_meta_user()

        # Ingest the census blocks
        init_census()

        # TODO: support local ingest of csv
        # For now, ingest Chicago's csv of 2013 flu shot locations from the data portal.
        # It's a nice little Chicago dataset that won't change.

        # So, adding the dataset to meta_table happens in view.py.
        # I don't want to mock out a whole response object with form data and such,
        # so here's a janky way.
        url = 'https://data.cityofchicago.org/api/views/g5vx-5vqf/rows.csv?accessType=DOWNLOAD'
        url_hash = md5(url).hexdigest()

        d = {
            'dataset_name': u'flu_shot_clinic_locations',
            'human_name': u'flu_shot_clinic_locations',
            'attribution': u'foo',
            'description': u'bar',
            'source_url': url,
            'source_url_hash': url_hash,
            'update_freq': 'yearly',
            'business_key': u'Event',
            'observed_date': u'Date',
            'latitude': u'Latitude',
            'longitude': u'Longitude',
            'location': u'Location',
            'contributor_name': u'Frederick Mcgillicutty',
            'contributor_organization': u'StrexCorp',
            'contributor_email': u'foo@bar.edu',
            'contributed_data_types': None,
            'approved_status': True,
            'is_socrata_source': False
        }

        # add this to meta_master
        md = MetaTable(**d)
        session.add(md)
        session.commit()

        meta = {
            'dataset_name': u'flu_shot_clinic_locations',
            'source_url': url,
            'business_key': u'Event',
            'observed_date': u'Date',
            'latitude': u'Latitude',
            'longitude': u'Longitude',
            'location': u'Location',
            'source_url_hash': url_hash

        }
        point_etl = PlenarioETL(meta)
        point_etl.add()

        cls.app = create_app().test_client()

    def test_point_dataset_visible(self):
        resp = self.app.get('/v1/api/fields/flu_shot_clinic_locations')
        self.assertEqual(resp.status_code, 200)

    def test_filter_on_census_block(self):

        # Query the flu_shot_clinic_locations dataset and return the number of records retrieved.
        # Filter by census_block__like=census_block_string.
        def num_rows_returned_from_detail_query(census_block_string):
            request = '/v1/api/detail/' +\
                      '?dataset_name=flu_shot_clinic_locations' +\
                      '&obs_date__ge=2013-1-1' +\
                      '&census_block__like=' + census_block_string
            resp = self.app.get(request)
            self.assertEqual(resp.status_code, 200)
            data = json.loads(resp.data)
            return len(data['objects'])

        # I'm going to pick on this row
        # 11/16/2013,10am,2pm,Saturday,7th Ward Burnham Math and Science Academy,Alderman,9928 S. Crandon,Chicago,IL,60649,(773) 731-7777,51,SOUTH DEERING,7,41.7144017635,-87.5671672122,"(41.7144017635, -87.5671672122)"
        # of the flu dataset.
        # Burnham Math and Science Academy (9928 S. Crandon Ave) is in census block 170315103004012
        self.assertEqual(1, num_rows_returned_from_detail_query('170315103004012'))
        # There was not a flu clinic in the census block nextdoor, 170315103004021
        self.assertEqual(0, num_rows_returned_from_detail_query('170315103004021'))
        # And we should be able to see the record if we zoom out to the tract level.
        # The clinic at the Burnham school was the only one in South Deering, so we expect just one record.
        self.assertEqual(1, num_rows_returned_from_detail_query('17031510300%'))