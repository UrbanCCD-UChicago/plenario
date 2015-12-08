import unittest
from tests.test_fixtures.point_meta import flu_shot_meta, landmarks_meta
from plenario.models import MetaTable
from plenario.database import session, app_engine
from plenario.etl.point import PlenarioETL
from init_db import init_master_meta_user
from sqlalchemy import Table, MetaData
from sqlalchemy.exc import NoSuchTableError
from plenario import create_app
import json
import os
import urllib


def ingest_online_from_fixture(fixture_meta):
        md = MetaTable(**fixture_meta)
        session.add(md)
        session.commit()
        point_etl = PlenarioETL(md)
        point_etl.add()


def drop_tables(table_names):
    drop_template = 'DROP TABLE IF EXISTS {};'
    command = ''.join([drop_template.format(table_name) for table_name in table_names])
    session.execute(command)
    session.commit()


def create_dummy_census_table():
    session.execute("""
    CREATE TABLE census_blocks
    (
        geoid10 INT,
        geom geometry(MultiPolygon,4326)
    );
    """)
    session.commit()


def get_loop_rect():
    pwd = os.path.dirname(os.path.realpath(__file__))
    rect_path = os.path.join(pwd, '../test_fixtures', 'loop_rectangle.json')
    with open(rect_path, 'r') as rect_json:
        query_rect = rect_json.read()
    escaped_query_rect = urllib.quote(query_rect)
    return escaped_query_rect


class DetailTests(unittest.TestCase):

    # Assume same setup as Timeseries
    @classmethod
    def setUpClass(cls):
        cls.app = create_app().test_client()

    def test_time_filter(self):
        query = '/v1/api/detail/?dataset_name=flu_shot_clinics&obs_date__ge=2013-09-22&obs_date__le=2013-10-1'
        resp = self.app.get(query)
        response_data = json.loads(resp.data)

        self.assertEqual(response_data['meta']['total'], 5)

    def test_space_filter(self):
        escaped_query_rect = get_loop_rect()

        url = '/v1/api/detail/?dataset_name=flu_shot_clinics&obs_date__ge=2013-01-01&obs_date__le=2013-12-31&location_geom__within=' + escaped_query_rect
        resp = self.app.get(url)
        response_data = json.loads(resp.data)
        self.assertEqual(response_data['meta']['total'], 5)

    # TODO: Verify CSV output
    '''def test_csv(self):
        query = '/v1/api/detail/?dataset_name=flu_shot_clinics&obs_date__ge=2013-09-22&obs_date__le=2013-10-1&data_type=csv'
        resp = self.app.get(query)
        print resp.data'''


class GridTests(unittest.TestCase):
    # Assume same setup as Timeseries
    @classmethod
    def setUpClass(cls):
        cls.app = create_app().test_client()

    def test_space_and_time(self):
        escaped_query_rect = get_loop_rect()
        query = 'v1/api/grid/?obs_date__ge=2013-1-1&obs_date_le=2014-1-1&dataset_name=flu_shot_clinics&location_geom__within=' + escaped_query_rect
        resp = self.app.get(query)
        response_data = json.loads(resp.data)
        print response_data
        self.assertEqual(len(response_data['features']), 4)

        # Each feature should have an associated square geometry with 5 points
        # (4 points to a square, then repeat the first to close it)
        squares = [feat['geometry']['coordinates'][0] for feat in response_data['features']]
        self.assert_(all([len(square) == 5 for square in squares]))

        # Each feature also needs a count of items found in that square.
        # We expect 3 squares with 1 and 1 square with 2
        counts = [feat['properties']['count'] for feat in response_data['features']]
        self.assertEqual(counts.count(1), 3)
        self.assertEqual(counts.count(2), 1)


class TimeseriesRegressionTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        tables_to_drop = [
            'flu_shot_clinics',
            'landmarks',
            'dat_master',
            'meta_master',
            'plenario_user'
        ]
        drop_tables(tables_to_drop)

        init_master_meta_user()

        # Make sure that it at least _looks_ like we have census blocks
        try:
            Table('census_blocks', MetaData(app_engine))
        except NoSuchTableError:
            create_dummy_census_table()

        ingest_online_from_fixture(flu_shot_meta)
        ingest_online_from_fixture(landmarks_meta)

        cls.app = create_app().test_client()

    def flu_agg(self, agg_type, expected_counts):
        # Always query from 9-22 to 10-1
        query = '/v1/api/timeseries/?obs_date__ge=2013-09-22&obs_date__le=2013-10-1&agg=' + agg_type
        resp = self.app.get(query)
        response_data = json.loads(resp.data)

        # Only the flu dataset should have records in this range
        self.assertEqual(len(response_data['objects']), 1)
        timeseries = response_data['objects'][0]
        self.assertEqual(timeseries['dataset_name'], 'flu_shot_clinics')

        # Extract the number of flu clinics per time unit
        counts = [time_unit['count'] for time_unit in timeseries['items']]
        self.assertEqual(expected_counts, counts)

    def test_day_agg(self):
        # 1 clinic on the 22nd. No clinics on the 23rd...
        expected_counts = [1, 0, 0, 0, 0, 0, 1, 0, 1, 2]
        self.flu_agg('day', expected_counts)

    def test_week_agg(self):
        # Weeks start from the beginning of the year, not the date specified in the query.
        # So even though we're only asking for 10 days,
        # we intersect with 3 weeks.
        expected_counts = [1, 1, 3]
        self.flu_agg('week', expected_counts)

    def test_month_agg(self):
        # 3 clinics in the range we specified in September, 2 in October.
        expected_counts = [3, 2]
        self.flu_agg('month', expected_counts)

    def test_year_agg(self):
        # 5 clinics when grouping by year.
        expected_counts = [5]
        self.flu_agg('year', expected_counts)

    def test_two_datasets(self):
        # Query over all of 2012 and 2013, aggregating by year.
        resp = self.app.get('/v1/api/timeseries/?obs_date__ge=2012-01-01&obs_date__le=2013-12-31&agg=year')
        response_data = json.loads(resp.data)

        # The order of the datasets isn't guaranteed, so preprocess the response
        # so we can grab each dataset's timeseries by name.
        name_to_series = {}
        for obj in response_data['objects']:
            timeseries = [year['count'] for year in obj['items']]
            name_to_series[obj['dataset_name']] = timeseries

        # 7 landmarks declared in 2012, 0 in 2013.
        self.assertEqual(name_to_series['landmarks'], [7, 0])
        # No flu shot clinics in 2012, 65 in 2013.
        self.assertEqual(name_to_series['flu_shot_clinics'], [0, 65])

    def test_geo_filter(self):
        escaped_query_rect = get_loop_rect()

        url = '/v1/api/timeseries/?obs_date__ge=2013-01-01&obs_date__le=2013-12-31&agg=year&location_geom__within=' + escaped_query_rect
        resp = self.app.get(url)
        resp_data = json.loads(resp.data)

        self.assertEqual(len(resp_data['objects']), 1)
        timeseries = resp_data['objects'][0]
        self.assertEqual(timeseries['dataset_name'], 'flu_shot_clinics')

        # Extract the number of flu clinics per time unit
        counts = [time_unit['count'] for time_unit in timeseries['items']]
        self.assertEqual([5], counts)


