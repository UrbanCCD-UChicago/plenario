import json
import os
import unittest
import urllib
import zipfile
from StringIO import StringIO
from hashlib import md5

from init_db import init_master_meta_user, init_census
from plenario import create_app
from plenario.database import session, app_engine as engine
from plenario.etl.shape import ShapeETL
from plenario.models import MetaTable, ShapeMetadata
from plenario.etl.point import PlenarioETL
from plenario.utils.shapefile import Shapefile

pwd = os.path.dirname(os.path.realpath(__file__))
FIXTURE_PATH = os.path.join(pwd, '..', 'test_fixtures')


def drop_tables(table_names):
    drop_template = 'DROP TABLE IF EXISTS {};'
    command = ''.join([drop_template.format(table_name) for table_name in table_names])
    engine.execute(command)


class Fixture(object):
    def __init__(self, human_name, file_name):
        self.human_name = human_name
        self.table_name = ShapeMetadata.make_table_name(human_name)
        self.path = os.path.join(FIXTURE_PATH, file_name)

fixtures = {
    'city': Fixture(human_name=u'Chicago City Limits',
                    file_name='chicago_city_limits.zip'),
    'streets': Fixture(human_name=u'Pedestrian Streets',
                       file_name='chicago_pedestrian_streets.zip'),
    'zips': Fixture(human_name=u'Zip Codes',
                    file_name='chicago_zip_codes.zip')
}


class ShapeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):

        # Remove tables that we're about to recreate.
        # This doesn't happen in teardown because I find it helpful to inspect them in the DB after running the tests.
        meta_table_names = ['dat_master', 'meta_shape', 'meta_master', 'plenario_user']
        fixture_table_names = [fixture.table_name for key, fixture in fixtures.iteritems()]
        drop_tables(meta_table_names + fixture_table_names)

        # Re-add meta tables
        init_master_meta_user()

        # Fully ingest the fixtures
        ShapeTests.ingest_fixture(fixtures['city'])
        ShapeTests.ingest_fixture(fixtures['streets'])
        ShapeTests.ingest_fixture(fixtures['zips'])

        # Add a dummy dataset to the metadata without ingesting a shapefile for it
        cls.dummy_name = ShapeMetadata.add(caller_session=session, human_name=u'Dummy Name', source_url=None).dataset_name
        session.commit()

        cls.app = create_app().test_client()

    @staticmethod
    def ingest_fixture(fixture):
        # Add the fixture to the metadata first
        shape_meta = ShapeMetadata.add(caller_session=session, human_name=fixture.human_name, source_url=None)
        session.commit()
        # Bypass the celery task and call on a ShapeETL directly
        ShapeETL(meta=shape_meta, source_path=fixture.path).import_shapefile()
        return shape_meta

    def test_names_in_shape_list(self):
        resp = self.app.get('/v1/api/shapes/')
        response_data = json.loads(resp.data)
        all_names = [item['dataset_name'] for item in response_data['objects']]

        # Are all the names of the fully ingested fixtures in the response?
        fixture_names_included = [(fixture.table_name in all_names) for fixture in fixtures.values()]
        self.assertTrue(all(fixture_names_included))

        # And make sure the name of an uningested shape didn't sneak in.
        self.assertNotIn(self.dummy_name, all_names)

    def test_find_intersecting(self):
        # See test_fixtures/README for a picture of the rectangle
        rect_path = os.path.join(FIXTURE_PATH, 'university_village_rectangle.json')
        with open(rect_path, 'r') as rect_json:
            query_rect = rect_json.read()
        escaped_query_rect = urllib.quote(query_rect)

        # What shape datasets intersect with the rectangle?
        resp = self.app.get('/v1/api/shapes/intersections/' + escaped_query_rect)
        self.assertEqual(resp.status_code, 200)
        response_data = json.loads(resp.data)

        # By design, the query rectangle should cross 3 zip codes and 2 pedestrian streets
        datasets_to_num_geoms = {obj['dataset_name']: obj['num_geoms'] for obj in response_data['objects']}
        self.assertEqual(datasets_to_num_geoms[fixtures['zips'].table_name], 3)
        self.assertEqual(datasets_to_num_geoms[fixtures['streets'].table_name], 2)

    def test_export_geojson(self):
        # Do we at least get some json back?
        resp = self.app.get('/v1/api/shapes/{}?data_type=json'.format(fixtures['city'].table_name))
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
        resp = self.app.get('/v1/api/shapes/this_is_a_fake_name')
        self.assertEqual(resp.status_code, 404)

    def test_export_shapefile(self):
        resp = self.app.get('/v1/api/shapes/{}?data_type=shapefile'.format(fixtures['city'].table_name))
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.content_type, 'application/zip')
        file_content = StringIO(resp.data)
        as_zip = zipfile.ZipFile(file_content)

        # The Shapefile utility class takes a ZipFile, opens it,
        # and throws an exception if it doesn't have the expected shapefile components (.shp and .prj namely)
        with Shapefile(as_zip):
            pass

    def test_export_kml(self):
        resp = self.app.get('/v1/api/shapes/{}?data_type=kml'.format(fixtures['city'].table_name))
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.content_type, 'application/vnd.google-earth.kml+xml')

        # Don't have good automated way to test that it gives valid KML :(

    def test_no_import_when_name_conflict(self):
        # The city fixture should already be ingested
        with self.assertRaises(Exception):
            ShapeTests.ingest_fixture(fixtures['city'])
        session.rollback()

    def test_uningested_shape_unavailable_for_export(self):
        resp = self.app.get('/v1/api/shapes/' + self.dummy_name)
        self.assertEqual(resp.status_code, 404)

    def test_delete_shape(self):
        # Can we remove a shape that's fully ingested?
        city_meta = session.query(ShapeMetadata).get(fixtures['city'].table_name)
        self.assertIsNotNone(city_meta)
        city_meta.remove_table(caller_session=session)
        session.commit()
        city_meta = session.query(ShapeMetadata).get(fixtures['city'].table_name)
        self.assertIsNone(city_meta)

        # Can we remove a shape that's only in the metadata?
        dummy_meta = session.query(ShapeMetadata).get(self.dummy_name)
        self.assertIsNotNone(dummy_meta)
        dummy_meta.remove_table(caller_session=session)
        session.commit()
        dummy_meta = session.query(ShapeMetadata).get(self.dummy_name)
        self.assertIsNone(dummy_meta)

        # Add them back to return to original test state
        ShapeTests.ingest_fixture(fixtures['city'])
        ShapeMetadata.add(caller_session=session, human_name=u'Dummy Name', source_url=None)
        session.commit()

class CensusRegressionTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Assume there exists a test database with postgis at the connection string specified in test_settings.py
        tables_to_drop = ['census_blocks',
                          'dat_flu_shot_clinic_locations',
                          'dat_master',
                          'meta_master',
                          'meta_shape',
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