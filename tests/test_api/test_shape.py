import json
import os
import urllib.request, urllib.parse, urllib.error
import zipfile
from io import BytesIO

from plenario.database import postgres_session, postgres_engine as engine
from plenario.models import ShapeMetadata
from plenario.etl import ingest_shapes
from plenario.utils.shapefile import Shapefile
from tests.fixtures.base_test import BasePlenarioTest, FIXTURE_PATH, \
    shape_fixtures


class ShapeTests(BasePlenarioTest):

    @classmethod
    def setUpClass(cls):
        super(ShapeTests, cls).setUpClass(shutdown=True)
        cls.ingest_shapes()
        cls.ingest_points()

    ''' /etl '''

    def test_update(self):
        # Try to ingest slightly changed shape
        fixture = shape_fixtures['changed_neighborhoods']
        # Add the fixture to the registry first
        shape_meta = postgres_session.query(ShapeMetadata).get('chicago_neighborhoods')
        # Do a ShapeETL update
        ingest_shapes(meta=shape_meta, local=True)
        t = shape_meta.shape_table
        sel = t.select().where(t.c['sec_neigh'] == 'ENGLEWOOD')
        res = engine.execute(sel).fetchall()
        altered_value = res[0]['pri_neigh']
        # I changed Englewood to Englerwood :P
        self.assertEqual(altered_value, 'Englerwood')

    def test_no_import_when_name_conflict(self):
        # The city fixture should already be ingested
        with self.assertRaises(Exception):
            ShapeTests.ingest_fixture(shape_fixtures['city'])
        postgres_session.rollback()

    def test_delete_shape(self):
        # Can we remove a shape that's fully ingested?
        city_meta = postgres_session.query(ShapeMetadata).get(shape_fixtures['city'].table_name)
        self.assertIsNotNone(city_meta)
        city_meta.remove_table()
        postgres_session.commit()
        city_meta = postgres_session.query(ShapeMetadata).get(shape_fixtures['city'].table_name)
        self.assertIsNone(city_meta)

        # Can we remove a shape that's only in the metadata?
        dummy_meta = postgres_session.query(ShapeMetadata).get(self.dummy_name)
        self.assertIsNotNone(dummy_meta)
        dummy_meta.remove_table()
        postgres_session.commit()
        dummy_meta = postgres_session.query(ShapeMetadata).get(self.dummy_name)
        self.assertIsNone(dummy_meta)

        # Add them back to return to original test state
        ShapeTests.ingest_fixture(shape_fixtures['city'])
        ShapeMetadata.add(human_name='Dummy Name',
                          source_url=None,
                          update_freq='yearly',
                          approved_status=False)

        postgres_session.commit()

    '''/shapes'''

    def test_names_in_shape_list(self):
        resp = self.app.get('/v1/api/shapes/')
        response_data = json.loads(bytes.decode(resp.data))
        all_names = [item['dataset_name'] for item in response_data['objects']]

        # Are all the names of the fully ingested fixtures in the response?
        fixture_names_included = [(fixture.table_name in all_names) for fixture in list(shape_fixtures.values())]
        self.assertTrue(all(fixture_names_included))

        # And make sure the name of an uningested shape didn't sneak in.
        self.assertNotIn(self.dummy_name, all_names)

    def test_num_shapes_in_meta(self):
        resp = self.app.get('/v1/api/shapes/')
        response_data = json.loads(bytes.decode(resp.data))

        # Expect field called num_shapes for each metadata object
        # Will throw KeyError if 'num_shapes' not found in each
        shape_nums = {obj['dataset_name']: obj['num_shapes'] for obj in response_data['objects']}

        self.assertEqual(shape_nums['chicago_city_limits'], 1)
        self.assertEqual(shape_nums['zip_codes'], 61)
        self.assertEqual(shape_nums['pedestrian_streets'], 41)

    def test_column_metadata(self):
        resp = self.app.get('/v1/api/shapes/')
        response_data = json.loads(bytes.decode(resp.data))

        limits = filter(
            lambda dset: dset['dataset_name'] == 'chicago_city_limits',
            response_data['objects']
        )
        limits = list(limits)[0]
        self.assertEqual(4, len(limits['columns']))


    ''' /intersections '''

    def test_find_intersecting(self):
        # See test_fixtures/README for a picture of the rectangle
        rect_path = os.path.join(FIXTURE_PATH, 'university_village_rectangle.json')
        with open(rect_path, 'r') as rect_json:
            query_rect = rect_json.read()
        escaped_query_rect = urllib.parse.quote(query_rect)

        # Moving this functionality to the /shapes endpoint
        # What shape datasets intersect with the rectangle?
        resp = self.app.get('/v1/api/shapes/?location_geom__within=' + escaped_query_rect)
        self.assertEqual(resp.status_code, 200)
        response_data = json.loads(bytes.decode(resp.data))

        # By design, the query rectangle should cross 3 zip codes and 2 pedestrian streets
        datasets_to_num_geoms = {obj['dataset_name']: obj['num_shapes'] for obj in response_data['objects']}
        self.assertEqual(datasets_to_num_geoms[shape_fixtures['zips'].table_name], 3)
        self.assertEqual(datasets_to_num_geoms[shape_fixtures['streets'].table_name], 2)

    # =================================
    # /shapes/<shape_name> tree filters
    # =================================

    def test_shapes_with_simple_filter(self):
        url = '/v1/api/shapes/pedestrian_streets?pedestrian_streets__filter=' \
              '{"op": "eq", "col": "name", "val": "PEDESTRIAN STREET"}'
        resp = self.app.get(url)
        data = json.loads(bytes.decode(resp.data))
        self.assertEqual(len(data['features']), 21)

    def test_shapes_with_compound_filter(self):
        url = '/v1/api/shapes/pedestrian_streets?pedestrian_streets__filter=' \
              '{"op": "or", "val": [' \
                   '{"op": "ge", "col": "ogc_fid", "val": 20},' \
                   '{"op": "eq", "col": "name", "val": "PEDESTRIAN STREET"}' \
              ']}'
        resp = self.app.get(url)
        data = json.loads(bytes.decode(resp.data))
        self.assertEqual(len(data['features']), 33)

    def test_shapes_with_nested_compound_filter(self):
        url = '/v1/api/shapes/pedestrian_streets?pedestrian_streets__filter=' \
              '{"op": "or", "val": [' \
                  '{"op": "le", "col": "ogc_fid", "val": 10},' \
                  '{"op": "and", "val": [' \
                      '{"op": "ge", "col": "ogc_fid", "val": 20},' \
                      '{"op": "eq", "col": "name", "val": "PEDESTRIAN STREET"}' \
                  ']}' \
              ']}'
        resp = self.app.get(url)
        data = json.loads(bytes.decode(resp.data))
        self.assertEqual(len(data['features']), 20)

    def test_export_geojson(self):
        # Do we at least get some json back?
        resp = self.app.get('/v1/api/shapes/{}?data_type=json'.format(shape_fixtures['city'].table_name))
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.content_type, 'application/json')

        # Dropping the fixture shapefile into QGIS shows that there are 13,403 points in the multipolygon.
        expected_num_points = 13403

        # Do we get 13,403 points back?
        city_geojson = json.loads(bytes.decode(resp.data))
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
        resp = self.app.get('/v1/api/shapes/{}?data_type=shapefile'.format(shape_fixtures['city'].table_name))
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.content_type, 'application/zip')
        file_content = BytesIO(resp.data)
        as_zip = zipfile.ZipFile(file_content)

        # The Shapefile utility class takes a ZipFile, opens it,
        # and throws an exception if it doesn't have the expected shapefile components (.shp and .prj namely)
        with Shapefile(as_zip):
            pass

    def test_export_kml(self):
        resp = self.app.get('/v1/api/shapes/{}?data_type=kml'.format(shape_fixtures['city'].table_name))
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.content_type, 'application/vnd.google-earth.kml+xml')
        # Don't have good automated way to test that it gives valid KML :(

    def test_uningested_shape_unavailable_for_export(self):
        resp = self.app.get('/v1/api/shapes/' + self.dummy_name)
        self.assertEqual(resp.status_code, 404)

    '''/filter'''

    def test_filter_with_pedestrian_streets_in_university_village(self):
        rect_path = os.path.join(FIXTURE_PATH, 'university_village_rectangle.json')
        with open(rect_path, 'r') as rect_json:
            query_rect = rect_json.read()
        escaped_query_rect = urllib.parse.quote(query_rect)

        #url = '/v1/api/shapes/filter/pedestrian_streets/' + query_rect
        url = '/v1/api/shapes/pedestrian_streets/?location_geom__within=' + escaped_query_rect
        response = self.app.get(url)
        self.assertEqual(response.status_code, 200)

        data = json.loads(bytes.decode(response.data))
        streets = data['features']
        self.assertEqual(len(streets), 2)

    def test_filter_with_pedestrian_streets_in_loop(self):
        rect_path = os.path.join(FIXTURE_PATH, 'loop_rectangle.json')
        with open(rect_path, 'r') as rect_json:
            query_rect = rect_json.read()
        escaped_query_rect = urllib.parse.quote(query_rect)

        #url = '/v1/api/shapes/filter/pedestrian_streets/' + query_rect
        url = '/v1/api/shapes/pedestrian_streets/?location_geom__within=' + escaped_query_rect
        response = self.app.get(url)
        self.assertEqual(response.status_code, 200)

        data = json.loads(bytes.decode(response.data))
        streets = data['features']
        self.assertEqual(len(streets), 6)

    def test_aggregate_point_data_with_landmarks_neighborhoods_and_time(self):
        url = '/v1/api/shapes/chicago_neighborhoods/landmarks/?obs_date__ge=2000-09-22&obs_date__le=2013-10-1'
        response = self.app.get(url)
        self.assertEqual(response.status_code, 200)

        data = json.loads(bytes.decode(response.data))
        neighborhoods = data['features']
        self.assertEqual(len(neighborhoods), 54)

        for neighborhood in neighborhoods:
            self.assertGreaterEqual(neighborhood['properties']['count'], 1)
            #print neighborhood['properties']['sec_neigh'], neighborhood['properties']['count']

    def test_aggregate_point_data_with_landmarks_neighborhoods_architect_and_time(self):
        url = '/v1/api/shapes/chicago_neighborhoods/landmarks/?obs_date__ge=1900-09-22&obs_date__le=2013-10-1&architect__in=Frank Lloyd Wright,Fritz Lang'
        response = self.app.get(url)
        self.assertEqual(response.status_code, 200)

        data = json.loads(bytes.decode(response.data))
        neighborhoods = data['features']
        self.assertEqual(len(neighborhoods), 6)
        
        for neighborhood in neighborhoods:
            self.assertGreaterEqual(neighborhood['properties']['count'], 1)
            #print neighborhood['properties']['sec_neigh'], neighborhood['properties']['count']

    def test_filter_point_data_with_landmarks_neighborhoods_and_bounding_box(self):
        rect_path = os.path.join(FIXTURE_PATH, 'loop_rectangle.json')
        with open(rect_path, 'r') as rect_json:
            query_rect = rect_json.read()

        url = '/v1/api/shapes/chicago_neighborhoods/landmarks/?obs_date__ge=1900-09-22&obs_date__le=2013-10-1&location_geom__within=' + query_rect
        response = self.app.get(url)
        data = json.loads(bytes.decode(response.data))
        neighborhoods = data['features']
        self.assertGreaterEqual(20, len(neighborhoods)) 
          #check that total number of neighborhoods does not exceed number within this bounding box (The Loop)

        for neighborhood in neighborhoods:
            self.assertGreaterEqual(neighborhood['properties']['count'], 1)
            #print neighborhood['properties']['sec_neigh'], neighborhood['properties']['count']

    def test_export_shape_with_location_filtering(self):
        rect_path = os.path.join(FIXTURE_PATH, 'loop_rectangle.json')
        with open(rect_path, 'r') as rect_json:
            query_rect = rect_json.read()
        escaped_query_rect = urllib.parse.quote(query_rect)
        unfiltered_url = '/v1/api/shapes/chicago_neighborhoods/'#?location_geom__within=' + escaped_query_rect
        filtered_url = '/v1/api/shapes/chicago_neighborhoods/?location_geom__within=' + escaped_query_rect
        
        unfiltered_response = self.app.get(unfiltered_url)
        unfiltered_data = json.loads(bytes.decode(unfiltered_response.data))
        unfiltered_neighborhoods = unfiltered_data['features']

        filtered_response = self.app.get(filtered_url)
        filtered_data = json.loads(bytes.decode(filtered_response.data))
        filtered_neighborhoods = filtered_data['features']
        self.assertGreater(len(unfiltered_neighborhoods), len(filtered_neighborhoods))

    def test_verify_result_columns(self):
        url = '/v1/api/shapes/chicago_neighborhoods/landmarks/?obs_date__ge=1900-09-22&obs_date__le=2013-10-1'
        response = self.app.get(url)
        data = json.loads(bytes.decode(response.data))
        for feature in data['features']:
            self.assertFalse(feature['properties'].get('hash'))
            self.assertFalse(feature['properties'].get('ogc_fid'))
            self.assertTrue(feature['properties'].get('count'))
