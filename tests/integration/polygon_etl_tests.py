import unittest
import os
from hashlib import md5
from plenario.utils.shapefile_helpers import PolygonETL, add_polygon_table_to_meta
from plenario.utils.etl import PlenarioETL, PlenarioETLError
from plenario import create_app
from plenario.database import task_engine, init_t_db
from plenario.database import task_session as session
from plenario.models import MetaTable
import json

# Assume that the Cook County census blocks
# (http://www2.census.gov/geo/tiger/TIGER2010/TABBLOCK/2010/tl_2010_17031_tabblock10.zip)
# are downloaded to test_fixtures as 'cook_county_census_blocks.zip'

pwd = os.path.dirname(os.path.realpath(__file__))
CENSUS_SHAPE_PATH = os.path.join(pwd, '..', 'test_fixtures', 'cook_county_census_blocks.zip')


def set_up_env():
    # Assume there exists a test database with postgis at the connection string specified in test_settings.py

    # Clean up messes.
    task_engine.execute("DROP TABLE IF EXISTS dat_census_blocks;\
                        DROP TABLE IF EXISTS dat_flu_shot_clinic_locations;\
                        DROP TABLE IF EXISTS dat_master;\
                        DROP TABLE IF EXISTS meta_master;\
                        DROP TABLE IF EXISTS plenario_user;")

    # Create meta and master
    init_t_db()

    # Ingest the census blocks locally
    polygon_etl = PolygonETL()
    print 'Importing census data the new way'

    polygon_etl.import_shapefile_local('census_blocks', CENSUS_SHAPE_PATH, 4269)
    print 'Trying to add the flipping metadata'
    add_polygon_table_to_meta('census_blocks',
                              'http://www2.census.gov/geo/tiger/TIGER2010/TABBLOCK/2010/tl_2010_17031_tabblock10.zip',
                              'geoid10')

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


class PolygonETLRegressionTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Maybe check if the expected test env exists.
        # If not, call setup
        cls.app = create_app().test_client()

    def test_point_dataset_visible(self):
        resp = self.app.get('/v1/api/fields/flu_shot_clinic_locations')
        self.assertEqual(resp.status_code, 200)

    def test_polygon_datset_visible_in_datasets(self):
        resp = self.app.get('/v1/api/datasets')
        num_datasets_visible = len(json.loads(resp.data)['objects'])
        self.assertEqual(num_datasets_visible, 2)

    def test_polygon_dataset_visible_in_dataset_fields(self):
        resp = self.app.get('/v1/api/fields/census_blocks')
        #print resp.data
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

    def test_no_import_when_name_conflict(self):
        polygon_etl = PolygonETL()
        print CENSUS_SHAPE_PATH
        self.assertRaises(PlenarioETLError,
                          polygon_etl.import_shapefile_local,'census_blocks', CENSUS_SHAPE_PATH, 4269)

