import unittest
import os
from hashlib import md5
from plenario.utils.shapefile_helpers import PolygonETL, polygon_source_has_changed
from plenario.utils.etl import PlenarioETL, PlenarioETLError
from plenario import create_app
from plenario.database import task_engine
from init_db import init_master_meta_user, init_census
from plenario.database import task_session as session
from plenario.models import MetaTable, PolygonDataset
import json

pwd = os.path.dirname(os.path.realpath(__file__))
FIXTURE_PATH = os.path.join(pwd, '..', 'test_fixtures')

# The shapefiles I'm using for testing are projected as NAD_1983_StatePlane_Illinois_East_FIPS_1201_Feet
ILLINOIS_STATE_PLANE_SRID = 3435


def drop_tables(table_names):
    drop_template = 'DROP TABLE IF EXISTS {};'
    command = ''.join([drop_template.format(table_name) for table_name in table_names])
    task_engine.execute(command)


class PolygonAPITests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Use Chicago's pedestrian streets to test how we handle polylines
        cls.lines_name = 'pedestrian_streets'
        cls.lines_srid = ILLINOIS_STATE_PLANE_SRID
        cls.lines_path = os.path.join(FIXTURE_PATH, 'chicago_pedestrian_streets.zip')

        # Use Chciago's zip codes to test how we handle polygons
        cls.polygons_name = 'zip_codes'
        cls.polygons_srid = ILLINOIS_STATE_PLANE_SRID
        cls.polygons_path = os.path.join(FIXTURE_PATH, 'chicago_zip_codes.zip')

        # Clean up
        tables_to_drop = [cls.lines_name, cls.polygons_name, 'dat_master', 'meta_polygon', 'meta_master', 'plenario_user']
        drop_tables(tables_to_drop)
        init_master_meta_user()

        # Ingest
        PolygonETL(cls.polygons_name, save_to_s3=False).import_shapefile(cls.polygons_srid,
                                                                         None,
                                                                         source_path=cls.polygons_path)

        PolygonETL(cls.lines_name, save_to_s3=False).import_shapefile(cls.lines_srid,
                                                                      None,
                                                                      source_path=cls.lines_path)

        cls.app = create_app().test_client()

    def test_names_in_polygon_list(self):
        resp = self.app.get('/v1/api/polygons/')
        response_data = json.loads(resp.data)
        all_names = [item['dataset_name'] for item in response_data['objects']]
        self.assertIn(self.lines_name, all_names)
        self.assertIn(self.polygons_name, all_names)


class PolygonETLTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Chicago's city limits is a relatively small polygon dataset. Nice for testing.
        cls.dataset_name = 'chicago_city_limits'
        cls.srid = ILLINOIS_STATE_PLANE_SRID

        # Clean our testing environment
        tables_to_drop = [cls.dataset_name, 'dat_master', 'meta_polygon', 'meta_master', 'plenario_user']
        drop_tables(tables_to_drop)
        init_master_meta_user()

        # Ingest locally
        cls.shapefile_path = os.path.join(FIXTURE_PATH, 'chicago_city_limits.zip')
        PolygonETL(cls.dataset_name, save_to_s3=False).import_shapefile(cls.srid,
                                                                        None,        # Don't care about source url
                                                                        source_path=cls.shapefile_path)

        # Different enough to change the hash, but should have the same data.
        cls.changed_shapefile_path = os.path.join(FIXTURE_PATH, 'chicago_city_limits_changed.zip')

        cls.app = create_app().test_client()

    def test_no_import_when_name_conflict(self):
        polygon_etl = PolygonETL(self.dataset_name)
        with self.assertRaises(PlenarioETLError):
            polygon_etl.import_shapefile(self.srid, 'dummy_business_key', 'dummy/path')

    def test_hash_invariant(self):
        from plenario.utils.shapefile_helpers import sha_hash_file
        self.assertEqual(sha_hash_file(self.shapefile_path),
                         sha_hash_file(self.shapefile_path))

    def test_hash_matches_when_source_file_is_the_same(self):
        self.assertFalse(polygon_source_has_changed(self.dataset_name, self.shapefile_path))

    def test_hash_does_not_match_when_source_file_is_different(self):
        self.assertTrue(polygon_source_has_changed(self.dataset_name, self.changed_shapefile_path))

    def test_update_when_source_file_changes(self):
        def get_update_timestamp():
            return session.query(PolygonDataset.last_update)\
                          .filter_by(dataset_name=self.dataset_name)\
                          .first()\
                          .last_update

        old_update_timestamp = get_update_timestamp()

        polygon_etl = PolygonETL(self.dataset_name, save_to_s3=False)
        polygon_etl.update_polygon_table(source_path=self.changed_shapefile_path)

        new_update_timestamp = get_update_timestamp()
        self.assertNotEqual(old_update_timestamp, new_update_timestamp)

    # Sloppy test
    '''def test_correct_attributes(self):
        resp = self.app.get('/v1/api/polygons/')
        self.assertEqual(resp.status_code, 200)
        dataset_attributes = json.loads(resp.data)['objects'][0].keys()
        expected_fields = ['source_srid', 'source_url', 'dataset_name', 'last_update', 'date_added']

        self.assertTrue(all([field in dataset_attributes for field in expected_fields]))'''

    def test_export_polygon_as_geojson(self):
        # Do we at least get some json back?
        resp = self.app.get('/v1/api/polygons/' + self.dataset_name)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.content_type, 'application/json')

        # Dropping the fixture shapefile into QGIS shows that there are 13,403 points in the multipolygon.
        expected_num_points = 13403

        # Do we get 13,403 point back?
        city_geojson = json.loads(resp.data)
        city_limits = city_geojson['features'][0]['geometry']['coordinates']
        observed_num_points = 0
        for outer_geom in city_limits:
            for inner_geom in outer_geom:
                observed_num_points += len(inner_geom)
        self.assertEqual(expected_num_points, observed_num_points)


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

    def test_census_dataset_visible_in_dataset_fields(self):
        resp = self.app.get('/v1/api/fields/census_blocks')
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