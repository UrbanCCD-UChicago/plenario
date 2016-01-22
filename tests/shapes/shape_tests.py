import json
import os
import unittest
import urllib
import zipfile
from StringIO import StringIO

from init_db import init_meta
from plenario import create_app
from plenario.database import session, app_engine as engine
from plenario.etl.shape import ShapeETL
from plenario.models import ShapeMetadata
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
        self.update_freq = 'yearly'

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
        meta_table_names = ['meta_shape']
        fixture_table_names = [fixture.table_name for key, fixture in fixtures.iteritems()]
        drop_tables(meta_table_names + fixture_table_names)

        # Re-add meta tables
        init_meta()

        # Fully ingest the fixtures
        ShapeTests.ingest_fixture(fixtures['city'])
        ShapeTests.ingest_fixture(fixtures['streets'])
        ShapeTests.ingest_fixture(fixtures['zips'])

        # Add a dummy dataset to the metadata without ingesting a shapefile for it
        cls.dummy_name = ShapeMetadata.add(human_name=u'Dummy Name',
                                           source_url=None,
                                           update_freq='yearly').dataset_name
        session.commit()

        cls.app = create_app().test_client()

    @staticmethod
    def ingest_fixture(fixture):
        # Add the fixture to the metadata first
        shape_meta = ShapeMetadata.add(human_name=fixture.human_name,
                                       source_url=None,
                                       update_freq=fixture.update_freq)
        session.commit()
        # Bypass the celery task and call on a ShapeETL directly
        ShapeETL(meta=shape_meta, source_path=fixture.path).ingest()
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

    def test_num_shapes_in_meta(self):
        resp = self.app.get('/v1/api/shapes/')
        response_data = json.loads(resp.data)

        # Expect field called num_shapes for each metadata object
        # Will throw KeyError if 'num_shapes' not found in each
        shape_nums = {obj['dataset_name']: obj['num_shapes'] for obj in response_data['objects']}

        self.assertEqual(shape_nums['chicago_city_limits'], 1)
        self.assertEqual(shape_nums['zip_codes'], 61)
        self.assertEqual(shape_nums['pedestrian_streets'], 41)

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
        city_meta.remove_table()
        session.commit()
        city_meta = session.query(ShapeMetadata).get(fixtures['city'].table_name)
        self.assertIsNone(city_meta)

        # Can we remove a shape that's only in the metadata?
        dummy_meta = session.query(ShapeMetadata).get(self.dummy_name)
        self.assertIsNotNone(dummy_meta)
        dummy_meta.remove_table()
        session.commit()
        dummy_meta = session.query(ShapeMetadata).get(self.dummy_name)
        self.assertIsNone(dummy_meta)

        # Add them back to return to original test state
        ShapeTests.ingest_fixture(fixtures['city'])
        ShapeMetadata.add(human_name=u'Dummy Name',
                          source_url=None,
                          update_freq='yearly')
        session.commit()

    '''/filter'''

    def test_filter(self):
        rect_path = os.path.join(FIXTURE_PATH, 'university_village_rectangle.json')
        with open(rect_path, 'r') as rect_json:
            query_rect = rect_json.read()

        url = '/v1/api/shapes/filter/chicago_pedestrian_streets/' + query_rect
        response = self.app.get(url)
        print response

        self.assertEqual(response.status_code, 200)
