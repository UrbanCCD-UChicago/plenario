import json
import os
import urllib
from StringIO import StringIO
import csv

from tests.test_fixtures.base_test import BasePlenarioTest, fixtures_path

# Filters
# =======
# Constants holding query string values, helps to have them all in one place.
# The row counts come from executing equivalents of these queries on posgres.

FLU_BASE = 'flu_shot_clinics__filter='
# Returns 4 rows for this condition.
FLU_FILTER_SIMPLE = '{"op": "eq", "col": "zip", "val": 60620}'
# Returns 10 rows.
FLU_FILTER_SIMPLE2 = '{"op": "eq", "col": "day", "val": "Wednesday"}'
# Returns 1 row.
FLU_FILTER_COMPOUND_AND = FLU_BASE + '{"op": "and", "val": [' + \
                          FLU_FILTER_SIMPLE + ', ' + \
                          FLU_FILTER_SIMPLE2 + ']}'
# Returns 13 rows.
FLU_FILTER_COMPOUND_OR = FLU_BASE + '{"op": "or", "val": [' + \
                          FLU_FILTER_SIMPLE + ', ' + \
                          FLU_FILTER_SIMPLE2 + ']}'
# Returns 4 rows.
FLU_FILTER_NESTED = '{"op": "and", "val": [' \
                    '   {"op": "ge", "col": "date", "val": "2013-11-01"},' \
                    '   {"op": "or", "val": [' + \
                            FLU_FILTER_SIMPLE + ', ' + \
                            FLU_FILTER_SIMPLE2 + \
                    '       ]' \
                    '   }' \
                    ']}'


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

    def get_api_response(self, query_string):
        """This bit of code seems to be repeated alot."""
        query = '/v1/api/' + query_string
        response = self.app.get(query)
        return json.loads(response.data)

    # ========
    # datasets
    # ========

    def test_metadata_no_args(self):
        r = self.get_api_response('datasets')
        self.assertEqual(len(r), 2)
        self.assertEqual(len(r['objects']), 3)

    def test_metadata_big_lower_bound(self):
        r = self.get_api_response('datasets?obs_date__ge=1000-01-01')
        self.assertEqual(len(r), 2)
        self.assertEqual(len(r['objects']), 3)

    def test_metadata_big_upper_bound(self):
        r = self.get_api_response('datasets?obs_date__le=2016-01-01')
        self.assertEqual(len(r), 2)
        self.assertEqual(len(r['objects']), 3)

    def test_metadata_both_bounds(self):
        r = self.get_api_response('datasets?obs_date__le=2016-01-01&obs_date__ge=2000-01-01')
        self.assertEqual(len(r), 2)
        self.assertEqual(len(r['objects']), 3)

    def test_metadata_single_dataset(self):
        r = self.get_api_response('datasets?dataset_name=crimes')
        self.assertEqual(len(r['objects']), 1)
        self.assertEqual(r['objects'][0]['view_url'],
                         "http://data.cityofchicago.org/api/views/ijzp-q8t2/rows")

    def test_metadata_filter(self):
        escaped_query_rect = get_loop_rect()
        query = 'datasets?location_geom__within={}'\
                '&obs_date__ge={}&obs_date__le={}'\
            .format(escaped_query_rect, '2015-1-1', '2016-1-1')
        r = self.get_api_response(query)
        self.assertEqual(len(r['objects']), 1)
        dataset_found = r['objects'][0]
        self.assertEqual(dataset_found['dataset_name'], 'crimes')

    def test_included_fields(self):
        query = '/v1/api/datasets/?dataset_name=flu_shot_clinics&include_columns=true'
        resp = self.app.get(query)
        response_data = json.loads(resp.data)
        cols = response_data['objects'][0]['columns']
        self.assertEqual(len(cols), 17)

    ''' /fields '''

    def test_fields(self):
        query = 'v1/api/fields/flu_shot_clinics'
        resp = self.app.get(query)
        response_data = json.loads(resp.data)

        # Should be the same length
        # as the number of columns in the source dataset
        self.assertEqual(len(response_data['objects']), 17)

    # ======
    # detail
    # ======

    def test_tree_filter_time(self):
        crimes_filter = '{"ge": ["point_date", "2014-12-12"]}'
        response = self.get_api_response('detail?crimes__filter=' + crimes_filter)
        self.assertEqual(len(response['objects']), 7)

    def test_validator_ignores_extra_filters(self):
        crimes_filter = '{"ge": ["point_date", "2014-12-12"]}'
        time_filter = '&obs_date__ge=2020-01-01'
        junk_filter = '&foo=bar'
        response = self.get_api_response('detail?crimes__filter=' +
                                         crimes_filter +
                                         time_filter +
                                         junk_filter)

        self.assertEqual(len(response['meta']['message']), 2)
        self.assertNotEquals(len(response['objects']), 0)

    def test_multiple_condition_trees(self):
        crimes_filter = '{"eq": ["description", "CREDIT CARD FRAUD"]}'
        flu_filter = '{"and":[{"ge":["point_date","2013-09-22"]},{"le":["point_date","2013-10-01"]}]}'

        response = self.get_api_response('detail?crimes__filter=' + crimes_filter +
                                         '&flu_shot_clinics__filter=' + flu_filter)

        # 2 from crimes, 5 from flu
        self.assertEquals(len(response['objects']), 7)

    def test_empty_condition_trees(self):
        response = self.get_api_response('detail?crimes__filter={}&flu_shot_clinics__filter={}')
        self.assertEquals(response['meta']['status'], 'error')

    def test_bad_condition_tree(self):
        response = self.get_api_response('detail?crimes__filter={"foo": "bar"})')
        self.assertEquals(response['meta']['status'], 'error')

    def test_malformed_condition_tree(self):
        response = self.get_api_response('detail?crimes__filter={"eq"": ["description", "CREDIT CARD FRAUD"]}')
        self.assertEquals(response['meta']['status'], 'error')

    def test_nonexistant_dataset(self):
        response = self.get_api_response('detail?foo__filter={"eq": ["id", 1]}')
        self.assertEquals(response['meta']['status'], 'error')

    def test_blank_dataset(self):
        response = self.get_api_response('detail?__filter={"eq": ["id", 1]}')
        self.assertEquals(response['meta']['status'], 'error')

    def test_detail_with_simple_flu_filter(self):
        r = self.get_api_response('detail?' + FLU_BASE + FLU_FILTER_SIMPLE)
        self.assertEqual(r['meta']['total'], 4)

    def test_detail_with_compound_flu_filters_and(self):
        r = self.get_api_response('detail?' + FLU_FILTER_COMPOUND_AND)
        self.assertEqual(r['meta']['total'], 1)

    def test_detail_with_compound_flu_filters_or(self):
        r = self.get_api_response('detail?' + FLU_FILTER_COMPOUND_OR)
        self.assertEqual(r['meta']['total'], 13)

    def test_detail_with_nested_flu_filters(self):
        r = self.get_api_response('detail?' + FLU_BASE + FLU_FILTER_NESTED)
        self.assertEqual(r['meta']['total'], 4)

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

    # ===========
    # /timeseries
    # ===========

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

    def test_timeseries_with_condition_tree(self):
        r = self.get_api_response('timeseries?'
                                  'crimes__filter={"eq": ["description", "CREDIT CARD FRAUD"]}'
                                  '&agg=year'
                                  '&obs_date__ge=2007')
        self.assertEqual(len(r['objects']), 1)

    # =================
    # /detail-aggregate
    # =================

    def test_detail_aggregate_with_just_lower_time_bound(self):

        resp = self.get_api_response('detail-aggregate?dataset_name=crimes&obs_date__ge=2015-01-01')
        self.assertEqual(resp['count'], 7)

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

    def test_aggregate_column_filter(self):
        query = 'v1/api/detail-aggregate/' \
                '?obs_date__ge=2013-1-1&obs_date__le=2014-1-1' \
                '&dataset_name=flu_shot_clinics&event_type=Church&agg=year'

        resp = self.app.get(query)
        response_data = json.loads(resp.data)
        # 6 Church-led flu shot clinics.
        self.assertEqual(response_data['objects'][0]['count'], 6)

    def test_aggregate_column_filter_tree(self):
        flu_filter = '{"eq": ["event_type", "Church"]}'
        response = self.get_api_response('detail-aggregate?obs_date__ge=2013&obs_date__le=2014'
                                         '&agg=year&flu_shot_clinics__filter=' +
                                         flu_filter)

        # 6 Church-led flu shot clinics.
        self.assertEqual(response['objects'][0]['count'], 6)

    def test_two_condition_trees(self):
        flu_filter = '{"eq": ["event_type", "Church"]}'
        crime_filter = '{"eq": ["description", "CREDIT CARD FRAUD"]}'
        response = self.get_api_response('detail-aggregate?'
                                         'flu_shot_clinics__filter=' + flu_filter +
                                         '&crimes__filter=' + crime_filter +
                                         '&agg=year&obs_date__ge=2012-01-01')

        # 6 from flu, 2 from crimes
        self.assertEqual(response['count'], 8)

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
