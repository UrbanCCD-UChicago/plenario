import json
import os
import urllib
from StringIO import StringIO
import csv

from tests.test_fixtures.base_test import BasePlenarioTest, fixtures_path


def get_escaped_geojson(fname):
    pwd = os.path.dirname(os.path.realpath(__file__))
    rect_path = os.path.join(pwd, '../test_fixtures', fname)
    with open(rect_path, 'r') as rect_json:
        query_rect = rect_json.read()
    escaped_query_rect = urllib.quote(query_rect)
    return escaped_query_rect


def get_loop_rect():
    return get_escaped_geojson('loop_rectangle.json')


class PointAPITests(BasePlenarioTest):

    @classmethod
    def setUpClass(cls):
        super(PointAPITests, cls).setUpClass()

    ''' /datasets '''

    def test_metadata_single(self):
        query = '/v1/api/datasets/?dataset_name=crimes'
        resp = self.app.get(query)
        response_data = json.loads(resp.data)

        self.assertEqual(len(response_data['objects']), 1)
        self.assertEqual(response_data['objects'][0]['view_url'],
                         "http://data.cityofchicago.org/api/views/ijzp-q8t2/rows")


    ''' /detail '''

    def test_time_filter(self):
        query = '/v1/api/detail/?dataset_name=flu_shot_clinics&obs_date__ge=2013-09-22&obs_date__le=2013-10-1'
        resp = self.app.get(query)
        response_data = json.loads(resp.data)

        self.assertEqual(response_data['meta']['total'], 5)

    def test_csv_response(self):
        query = '/v1/api/detail/?dataset_name=flu_shot_clinics&obs_date__ge=2013-09-22&obs_date__le=2013-10-1&data_type=csv'
        resp = self.app.get(query)

        mock_csv_file = StringIO(resp.data)
        reader = csv.reader(mock_csv_file)
        lines = [line for line in reader]
        # One header line, 5 data lines
        self.assertEqual(len(lines), 6)
        for line in lines:
            self.assertEqual(len(line), len(lines[0]))
        
        self.assertTrue('date' in lines[0])
        self.assertTrue('latitude' in lines[0])
        self.assertTrue('longitude' in lines[0])

    def test_geojson_response(self):
        query = '/v1/api/detail/?dataset_name=flu_shot_clinics&obs_date__ge=2013-09-22&obs_date__le=2013-10-1&data_type=geojson'
        resp = self.app.get(query)

        response_data = json.loads(resp.data)
        points = response_data['features']

        self.assertEqual(len(points), 5)
        attributes = points[0]
        self.assertTrue('geometry' in attributes)
        self.assertTrue('latitude' in attributes['properties'])
        self.assertTrue('longitude' in attributes['properties'])

    def test_space_filter(self):
        escaped_query_rect = get_loop_rect()

        url = '/v1/api/detail/?dataset_name=flu_shot_clinics&obs_date__ge=2013-01-01&obs_date__le=2013-12-31&location_geom__within=' + escaped_query_rect
        resp = self.app.get(url)
        response_data = json.loads(resp.data)
        self.assertEqual(response_data['meta']['total'], 5)

    def test_time_of_day(self):
        url = '/v1/api/detail/?dataset_name=crimes&obs_date__ge=2015-01-01&date__time_of_day_ge=6'
        resp = self.app.get(url)
        response_data = json.loads(resp.data)
        # Time of day filter should remove all but two
        self.assertEqual(response_data['meta']['total'], 2)

    def test_in_operator(self):
        url = '/v1/api/detail/?obs_date__le=2016%2F01%2F19&event_type__in=Alderman,CPD&obs_date__ge=2012%2F10%2F21&dataset_name=flu_shot_clinics'
        resp = self.app.get(url)
        response_data = json.loads(resp.data)
        self.assertEqual(response_data['meta']['total'], 53)

    def test_multipolygon(self):
        multipolygon = get_escaped_geojson('loop_and_near_southeast.json')
        url = 'v1/api/detail/?dataset_name=flu_shot_clinics&obs_date__ge=2013-01-01&obs_date__le=2013-12-31&location_geom__within=' + multipolygon
        resp = self.app.get(url)
        response_data = json.loads(resp.data)
        self.assertEqual(response_data['meta']['total'], 11)

    '''/grid'''

    def test_space_and_time(self):
        escaped_query_rect = get_loop_rect()
        query = 'v1/api/grid/?obs_date__ge=2013-1-1&obs_date__le=2014-1-1&dataset_name=flu_shot_clinics&location_geom__within=' + escaped_query_rect
        resp = self.app.get(query)
        response_data = json.loads(resp.data)
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

    def test_grid_column_filter(self):
        query = 'v1/api/grid/?obs_date__ge=2013-1-1&obs_date_le=2014-1-1' \
                '&dataset_name=flu_shot_clinics&event_type=Church'

        resp = self.app.get(query)
        response_data = json.loads(resp.data)
        # 6 Church-led flu shot clinics.
        # And they were far enough apart to each get their own square.
        self.assertEqual(len(response_data['features']), 6)

    '''/timeseries'''

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

    '''/detail-aggregate'''

    def test_aggregate(self):
        # Use same params as for timeseries
        query = '/v1/api/detail-aggregate/?dataset_name=flu_shot_clinics&obs_date__ge=2013-09-22&obs_date__le=2013-10-1&agg=week'
        resp = self.app.get(query)
        response_data = json.loads(resp.data)

        expected_counts = [1, 1, 3]
        observed_counts = [obj['count'] for obj in response_data['objects']]
        self.assertEqual(expected_counts, observed_counts)

    def test_polygon_filter(self):
        query = '/v1/api/detail/?dataset_name=flu_shot_clinics&obs_date__ge=2013-09-22&obs_date__le=2013-10-1&shape=chicago_neighborhoods'
        resp = self.app.get(query)
        response_data = json.loads(resp.data)

        self.assertEqual(response_data['meta']['total'], 5)

    def test_filter_point_data_with_landmarks_in_one_neighborhood(self):
        url = '/v1/api/detail/?dataset_name=landmarks&obs_date__ge=1900-09-22&obs_date__le=2013-10-1&shape=chicago_neighborhoods&sec_neigh__in=BRONZEVILLE'
        response = self.app.get(url)
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        points = data['objects']
        for point in points:
            self.assertEqual(point['chicago_neighborhoods.sec_neigh'], 'BRONZEVILLE')

    def test_aggregate_column_filter(self):
        query = 'v1/api/detail-aggregate/' \
                '?obs_date__ge=2013-1-1&obs_date_le=2014-1-1' \
                '&dataset_name=flu_shot_clinics&event_type=Church&agg=year'

        resp = self.app.get(query)
        response_data = json.loads(resp.data)
        # 6 Church-led flu shot clinics.
        self.assertEqual(response_data['objects'][0]['count'], 6)

    def test_bad_column_condition(self):
        query = 'v1/api/detail/?dataset_name=flu_shot_clinics&fake_column=fake'

        resp = self.app.get(query)
        response_data = json.loads(resp.data)
        self.assertTrue("Unused parameter value \"fake_column=fake\"" in response_data['meta']['message'])

    def test_bad_column_condition_with_shape(self):
        query = 'v1/api/detail/?dataset_name=flu_shot_clinics&shape=chicago_neighborhoods&fake_column=fake'

        resp = self.app.get(query)
        response_data = json.loads(resp.data)
        self.assertTrue("Unused parameter value \"fake_column=fake\"" in response_data['meta']['message'])