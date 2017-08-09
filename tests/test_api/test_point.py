import csv
import json
import os
import urllib.error
import urllib.parse
import urllib.request
from io import StringIO

from plenario.models import MetaTable

from tests.base import PlenarioTestCase


def ingest(fixture, name, url, date=None, location=None, latitude=None, longitude=None):
    source = os.path.join('tests', 'fixtures', fixture)

    MetaTable(
        human_name=name,
        observed_date=date,
        location=location,
        latitude=latitude,
        longitude=longitude,
        url=url
    ).load(source)


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
    rect_path = os.path.join(pwd, '../fixtures', fname)
    with open(rect_path, 'r') as rect_json:
        query_rect = rect_json.read()
    escaped_query_rect = urllib.parse.quote(query_rect)
    return escaped_query_rect


def get_loop_rect():
    return get_escaped_geojson('loop_rectangle.json')


class PointAPITests(PlenarioTestCase):

    def get_api_response(self, query_string):
        """This bit of code seems to be repeated alot."""
        query = '/v1/api/' + query_string
        response = self.app.get(query)
        return json.loads(response.data.decode("utf-8"))


    def test_metadata_single_dataset(self):
        r = self.get_api_response('datasets?dataset_name=crimes')
        self.assertEqual(len(r['objects']), 1)
        self.assertEqual(r['objects'][0]['view_url'],
                         "http://data.cityofchicago.org/api/views/ijzp-q8t2/rows")

    def test_metadata_filter(self):
        escaped_query_rect = get_loop_rect()
        query = 'datasets?location_geom__within={}' \
                '&obs_date__ge={}&obs_date__le={}' \
            .format(escaped_query_rect, '2015-1-1', '2016-1-1')
        r = self.get_api_response(query)
        self.assertEqual(len(r['objects']), 1)
        dataset_found = r['objects'][0]
        self.assertEqual(dataset_found['dataset_name'], 'crimes')

    def test_included_fields(self):
        r = self.get_api_response('datasets/?dataset_name=flu_shot_clinics'
                                  '&include_columns=true')
        self.assertEqual(len(r['objects'][0]['columns']), 17)

    ''' /fields '''

    def test_fields(self):
        r = self.get_api_response('fields/flu_shot_clinics')

        # Should be the same length
        # as the number of columns in the source dataset
        self.assertEqual(len(r['objects']), 17)

    # ====================
    # /detail tree filters
    # ====================

    def test_detail_with_simple_flu_filter(self):
        r = self.get_api_response(
            'detail?obs_date__ge=2000&dataset_name=flu_shot_clinics&' + FLU_BASE + FLU_FILTER_SIMPLE)
        self.assertEqual(r['meta']['total'], 4)

    def test_detail_with_compound_flu_filters_and(self):
        r = self.get_api_response('detail?obs_date__ge=2000&dataset_name=flu_shot_clinics&' + FLU_FILTER_COMPOUND_AND)
        self.assertEqual(r['meta']['total'], 1)

    def test_detail_with_compound_flu_filters_or(self):
        r = self.get_api_response('detail?obs_date__ge=2000&dataset_name=flu_shot_clinics&' + FLU_FILTER_COMPOUND_OR)
        self.assertEqual(r['meta']['total'], 13)

    def test_detail_with_nested_flu_filters(self):
        r = self.get_api_response(
            'detail?obs_date__ge=2000&dataset_name=flu_shot_clinics&' + FLU_BASE + FLU_FILTER_NESTED)
        self.assertEqual(r['meta']['total'], 4)

    # ============================
    # /detail query string filters
    # ============================

    def test_time_filter(self):
        r = self.get_api_response('detail?dataset_name=flu_shot_clinics'
                                  '&obs_date__ge=2013-09-22'
                                  '&obs_date__le=2013-10-1')
        self.assertEqual(r['meta']['total'], 5)

    def test_detail_with_0_hour_filter(self):
        endpoint = 'detail'
        dataset_arg = '?dataset_name=flu_shot_clinics'
        date_args = '&obs_date__ge=2013-09-22&obs_date__le=2013-10-1'
        hour_arg = '&date__time_of_day_ge=0'

        r = self.get_api_response(endpoint + dataset_arg + date_args + hour_arg)
        self.assertEqual(r['meta']['total'], 5)

    def test_detail_with_both_hour_filters(self):
        endpoint = 'detail'
        dataset_arg = '?dataset_name=crimes'
        date_args = '&obs_date__ge=2000'
        lower_hour_arg = '&date__time_of_day_ge=5'
        upper_hour_arg = '&date__time_of_day_le=17'

        r = self.get_api_response(endpoint + dataset_arg + date_args +
                                  upper_hour_arg + lower_hour_arg)
        self.assertEqual(r['meta']['total'], 3)

    def test_csv_response(self):
        query = '/v1/api/detail/?dataset_name=flu_shot_clinics&obs_date__ge=2013-09-22&obs_date__le=2013-10-1&data_type=csv'
        resp = self.app.get(query)

        mock_csv_file = StringIO(resp.data.decode("utf-8"))
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
        r = self.get_api_response('detail/?dataset_name=flu_shot_clinics'
                                  '&obs_date__ge=2013-09-22'
                                  '&obs_date__le=2013-10-1&data_type=geojson')

        points = r['features']

        self.assertEqual(len(points), 5)
        attributes = points[0]
        self.assertTrue('geometry' in attributes)
        self.assertTrue('latitude' in attributes['properties'])
        self.assertTrue('longitude' in attributes['properties'])

    def test_space_filter(self):
        escaped_query_rect = get_loop_rect()

        r = self.get_api_response('detail/?dataset_name=flu_shot_clinics'
                                  '&obs_date__ge=2013-01-01'
                                  '&obs_date__le=2013-12-31'
                                  '&location_geom__within=' + escaped_query_rect)
        self.assertEqual(r['meta']['total'], 5)

    def test_time_of_day(self):
        r = self.get_api_response('detail/?dataset_name=crimes'
                                  '&obs_date__ge=2015-01-01'
                                  '&date__time_of_day_ge=6')
        self.assertEqual(r['meta']['total'], 2)

    def test_in_operator(self):
        r = self.get_api_response('detail/?obs_date__le=2016%2F01%2F19'
                                  '&event_type__in=Alderman,CPD'
                                  '&obs_date__ge=2012%2F10%2F21'
                                  '&dataset_name=flu_shot_clinics')
        self.assertEqual(r['meta']['total'], 53)

    def test_multipolygon(self):
        multipolygon = get_escaped_geojson('loop_and_near_southeast.json')
        r = self.get_api_response('detail/?dataset_name=flu_shot_clinics'
                                  '&obs_date__ge=2013-01-01'
                                  '&obs_date__le=2013-12-31'
                                  '&location_geom__within=' + multipolygon)
        self.assertEqual(r['meta']['total'], 11)

    # ==================
    # /grid tree filters
    # ==================

    def test_grid_with_simple_tree_filter(self):
        filter_ = 'crimes__filters={"op": "eq", "col": "description", "val": "CREDIT CARD FRAUD"}'
        r = self.get_api_response('grid?obs_date__ge=2000&dataset_name=crimes&{}'.format(filter_))
        self.assertEqual(len(r['features']), 2)

    def test_space_and_time(self):
        escaped_query_rect = get_loop_rect()
        r = self.get_api_response('grid/'
                                  '?obs_date__ge=2013-1-1&obs_date__le=2014-1-1'
                                  '&dataset_name=flu_shot_clinics'
                                  '&location_geom__within=' + escaped_query_rect)
        self.assertEqual(len(r['features']), 4)

        # Each feature should have an associated square geometry with 5 points
        # (4 points to a square, then repeat the first to close it)
        squares = [feat['geometry']['coordinates'][0] for feat in r['features']]
        self.assertTrue(all([len(square) == 5 for square in squares]))

        # Each feature also needs a count of items found in that square.
        # We expect 3 squares with 1 and 1 square with 2
        counts = [feat['properties']['count'] for feat in r['features']]
        self.assertEqual(counts.count(1), 3)
        self.assertEqual(counts.count(2), 1)

    def test_grid_column_filter(self):
        r = self.get_api_response(
            'grid/?obs_date__ge=2013-1-1&obs_date_le=2014-1-1'
            '&dataset_name=flu_shot_clinics&event_type=Church'
        )

        # 6 Church-led flu shot clinics.
        # And they were far enough apart to each get their own square.
        self.assertEqual(len(r['features']), 6)

    # ===========
    # /timeseries
    # ===========

    def flu_agg(self, agg_type, expected_counts):
        # Always query from 9-22 to 10-1
        r = self.get_api_response(
            'timeseries/?obs_date__ge=2013-09-22'
            '&obs_date__le=2013-10-1&agg=' + agg_type
        )

        # Only the flu dataset should have records in this range
        self.assertEqual(len(r['objects']), 1)
        timeseries = r['objects'][0]
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

    def test_year_agg_csv(self):
        # Extend the test reach into the csv part of timeseries.
        query = '/v1/api/timeseries?obs_date__ge=2013-09-22&obs_date__le=2013-10-1&agg=year&data_type=csv'
        resp = self.app.get(query)

        # Assert a count of 5 in the year 2013.
        self.assertEqual(
            resp.data.decode("utf-8"),
            'temporal_group,flu_shot_clinics\r\n2013-01-01,5\r\n'
        )

    def test_two_datasets(self):
        # Query over all of 2012 and 2013, aggregating by year.
        r = self.get_api_response(
            'timeseries/?obs_date__ge=2012-01-01'
            '&obs_date__le=2013-12-31&agg=year'
        )

        # The order of the datasets isn't guaranteed, so preprocess the response
        # so we can grab each dataset's timeseries by name.
        name_to_series = {}
        for obj in r['objects']:
            timeseries = [year['count'] for year in obj['items']]
            name_to_series[obj['dataset_name']] = timeseries

        # 7 landmarks declared in 2012, 0 in 2013.
        self.assertEqual(name_to_series['landmarks'], [7, 0])
        # No flu shot clinics in 2012, 65 in 2013.
        self.assertEqual(name_to_series['flu_shot_clinics'], [0, 65])

    def test_geo_filter(self):
        escaped_query_rect = get_loop_rect()

        r = self.get_api_response(
            'timeseries/?obs_date__ge=2013-01-01&obs_date__le=2013-12-31'
            '&agg=year&location_geom__within=' + escaped_query_rect
        )

        self.assertEqual(len(r['objects']), 1)
        timeseries = r['objects'][0]
        self.assertEqual(timeseries['dataset_name'], 'flu_shot_clinics')

        # Extract the number of flu clinics per time unit
        counts = [time_unit['count'] for time_unit in timeseries['items']]
        self.assertEqual([5], counts)

    def test_timeseries_with_multiple_datasets(self):
        endpoint = 'timeseries'
        query = '?obs_date__ge=2000-08-01&agg=year&dataset_name__in=flu_shot_clinics,landmarks'

        resp_data = self.get_api_response(endpoint + query)
        print(resp_data)

        self.assertEqual(resp_data['objects'][0]['count'], 65)
        self.assertEqual(resp_data['objects'][1]['count'], 149)

    def test_timeseries_with_multiple_datasets_but_one_is_bad(self):
        endpoint = 'timeseries'
        query = '?obs_date__ge=2000&agg=year&dataset_name__in=flu_shot_clinics,landmarkz'

        resp_data = self.get_api_response(endpoint + query)

        self.assertIn('landmarkz', resp_data['meta']['message']['dataset_name__in']['1'][0])

    # ================================
    # /timeseries with condition trees
    # ================================

    def test_timeseries_with_a_tree_filter(self):
        endpoint = 'timeseries'
        query = '?obs_date__ge=2005-01-01&agg=year'
        qfilter = '&crimes__filter={"op": "eq", "col": "iucr", "val": 1150}'

        resp_data = self.get_api_response(endpoint + query + qfilter)

        # Crimes is the only one that gets a filter applied.
        self.assertEqual(resp_data['objects'][0]['count'], 2)
        self.assertEqual(resp_data['objects'][1]['count'], 65)
        self.assertEqual(resp_data['objects'][2]['count'], 88)

    def test_timeseries_with_multiple_filters(self):
        endpoint = 'timeseries'
        query = '?obs_date__ge=2005&agg=year'
        cfilter = '&crimes__filter={"op": "eq", "col": "iucr", "val": 1150}'
        lfilter = '&landmarks__filter={"op": "eq", "col": "architect", "val": "Frommann and Jebsen"}'

        resp_data = self.get_api_response(endpoint + query + cfilter + lfilter)

        # Crime filter gets applied.
        self.assertEqual(resp_data['objects'][0]['count'], 2)
        # Flu shots gets no filter applied.
        self.assertEqual(resp_data['objects'][1]['count'], 65)
        # Landmark filter gets applied.
        self.assertEqual(resp_data['objects'][2]['count'], 3)


class TestDatasetsApi(PlenarioTestCase):

    def test_endpoint_exists(self):
        response = self.client.get('/v1/api/datasets')
        self.assert200(response)

    def test_result_is_formatted_correctly(self):
        response = self.client.get('/v1/api/datasets')
        self.assertCountEqual(list(response.json.keys()), ['meta', 'objects'])
        self.assertCountEqual(list(response.json['meta'].keys()), ['status', 'query', 'total'])

    def test_result_has_correct_total(self):
        ingest(fixture='clinics.csv', name='Flu Shot Clinics', url='somewhere', date='date', location='location')
        ingest(fixture='clinics.csv', name='The Other Clinics', url='elsewhere', date='date', location='location')
        response = self.client.get('/v1/api/datasets')
        self.assertEquals(response.json['meta']['total'], 2)
        self.assertEquals(len(response.json['objects']), 2)

    def test_with_dataset_query_argument(self):
        ingest(fixture='clinics.csv', name='Flu Shot Clinics', url='somewhere', date='date', location='location')
        ingest(fixture='clinics.csv', name='The Other Clinics', url='elsewhere', date='date', location='location')
        response = self.client.get('/v1/api/datasets'
                                   '?dataset_name=the_other_clinics')
        self.assertEquals(response.json['meta']['total'], 1)
        self.assertEquals(len(response.json['objects']), 1)
        self.assertEquals(response.json['objects'][0]['human_name'], 'The Other Clinics')

    def test_with_invalid_dataset_query_argument(self):
        response = self.client.get('/v1/api/datasets'
                                   '?dataset_name=foo')
        self.assertEquals(len(response.json['errors']), 1)

    def test_with_obs_date__le_query_argument(self):
        ingest(fixture='clinics.csv', name='Flu Shot Clinics', url='somewhere', date='date', location='location')
        ingest(fixture='radios.csv', name='Community Radio Events', url='radios', date='date', latitude='lat', longitude='lon')
        response = self.client.get('/v1/api/datasets'
                                   '?obs_date__le=2015-01-01T00:00:00')
        self.assertEqual(len(response.json['objects']), 1)
        self.assertEqual(response.json['objects'][0]['human_name'], 'Flu Shot Clinics')

    def test_with_obs_date__ge_query_argument(self):
        ingest(fixture='clinics.csv', name='Flu Shot Clinics', url='somewhere', date='date', location='location')
        ingest(fixture='radios.csv', name='Community Radio Events', url='radios', date='date', latitude='lat', longitude='lon')
        response = self.client.get('/v1/api/datasets'
                                   '?obs_date__ge=2014-01-01T00:00:00')
        self.assertEqual(len(response.json['objects']), 1)
        self.assertEqual(response.json['objects'][0]['human_name'], 'Community Radio Events')

    def test_with_both_obs_date_arguments(self):
        ingest(fixture='clinics.csv', name='Flu Shot Clinics', url='somewhere', date='date', location='location')
        ingest(fixture='radios.csv', name='Community Radio Events', url='radios', date='date', latitude='lat', longitude='lon')
        ingest(fixture='landmarks.csv', name='Landmarks', url='landmarks', date='LANDMARK DESIGNATION DATE', location='Location')
        response = self.client.get('/v1/api/datasets'
                                   '?obs_date__ge=2012-07-01T00:00:00'
                                   '&obs_date__le=2014-01-01T00:00:00')
        self.assertEqual(len(response.json['objects']), 1)
        self.assertEqual(response.json['objects'][0]['human_name'], 'Flu Shot Clinics')

    def test_with_invalid_obs_date__ge(self):
        response = self.client.get('/v1/api/datasets'
                                   '?obs_date__ge=foo')
        self.assertEquals(len(response.json['errors']), 1)
        self.assertIn('obs_date__ge', response.json['errors'])

    def test_with_invalid_obs_date__le(self):
        response = self.client.get('/v1/api/datasets'
                                   '?obs_date__le=foo')
        self.assertEquals(len(response.json['errors']), 1)
        self.assertIn('obs_date__le', response.json['errors'])

    def test_with_empty_location_geom(self):
        ingest(fixture='landmarks.csv', name='Landmarks', url='landmarks', date='LANDMARK DESIGNATION DATE', location='Location')
        bounding_box = {
          "type": "FeatureCollection",
          "features": [
            {
              "type": "Feature",
              "properties": {},
              "geometry": {
                "type": "Polygon",
                "coordinates": [
                  [
                    [
                      -91.73583984374999,
                      41.343824581185686
                    ],
                    [
                      -90.28564453124999,
                      41.343824581185686
                    ],
                    [
                      -90.28564453124999,
                      42.66628070564928
                    ],
                    [
                      -91.73583984374999,
                      42.66628070564928
                    ],
                    [
                      -91.73583984374999,
                      41.343824581185686
                    ]
                  ]
                ]
              }
            }
          ]
        }

        bounding_box = json.dumps(bounding_box)
        response = self.client.get('/v1/api/datasets'
                                   '?location_geom__within={}'.format(bounding_box))
        self.assertEqual(len(response.json['objects']), 0)

    def test_with_correct_location_geom(self):
        ingest(fixture='landmarks.csv', name='Landmarks', url='landmarks', date='LANDMARK DESIGNATION DATE', location='Location')
        bounding_box = {
              "type": "FeatureCollection",
              "features": [
                {
                  "type": "Feature",
                  "properties": {},
                  "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                      [
                        [
                          -90.19775390625,
                          39.99395569397331
                        ],
                        [
                          -84.0234375,
                          39.99395569397331
                        ],
                        [
                          -84.0234375,
                          43.42898792344155
                        ],
                        [
                          -90.19775390625,
                          43.42898792344155
                        ],
                        [
                          -90.19775390625,
                          39.99395569397331
                        ]
                      ]
                    ]
                  }
                }
              ]
            }

        bounding_box = json.dumps(bounding_box)
        response = self.client.get('/v1/api/datasets'
                                       '?location_geom__within={}'.format(bounding_box))
        self.assertEqual(len(response.json['objects']), 1)

    def test_with_invalid_location_geom(self):
        ingest(fixture='landmarks.csv', name='Landmarks', url='landmarks', date='LANDMARK DESIGNATION DATE', location='Location')
        bounding_box = {
              "type": "FeatureCollection",
              "features": [
                {
                  "type": "Feature",
                  "properties": {},
                  "geometry": {
                    "type": "foo",
                    "coordinates": [
                      [
                        [
                          -90.19775390625,
                          39.99395569397331
                        ],
                        [
                          -84.0234375,
                          39.99395569397331
                        ],
                        [
                          -84.0234375,
                          43.42898792344155
                        ],
                        [
                          -90.19775390625,
                          43.42898792344155
                        ],
                        [
                          -90.19775390625,
                          39.99395569397331
                        ]
                      ]
                    ]
                  }
                }
              ]
            }
        bounding_box = json.dumps(bounding_box)
        response = self.client.get('/v1/api/datasets'
                                       '?location_geom__within={}'.format(bounding_box))

        self.assertEquals(len(response.json['errors']), 1)

class TestAggregateApi(PlenarioTestCase):

    def test_both_time_bounds(self):
        ingest_flu_shot_clinics()
        response = self.client.get('/v1/api/detail-aggregate'
                                   '?dataset_name=flu_shot_clinics'
                                   '&obs_date__ge=2013-09-22'
                                   '&obs_date__le=2013-10-01'
                                   '&agg=week')
        self.assertEqual([o['count'] for o in response.json['objects']], [1, 1, 3])

    def test_column_filter(self):
        ingest_flu_shot_clinics()
        response = self.client.get('v1/api/detail-aggregate'
                                   '?dataset_name=flu_shot_clinics'
                                   '&obs_date__le=2014-1-1'
                                   '&event_type=Church'
                                   '&agg=year')
        print(response)
        self.assertEqual(response.json['objects'][0]['count'], 6)

    def test_lower_time_bound(self):
        ingest_flu_shot_clinics()
        response = self.client.get('/v1/api/detail-aggregate'
                                   '?dataset_name=flu_shot_clinics'
                                   '&obs_date__ge=2013-10-02')
        self.assertEqual(response.json['count'], 60)


class TestDetailApi(PlenarioTestCase):

    def test_polygon_filter(self):
        ingest_flu_shot_clinics()
        response = self.client.get('/v1/api/detail'
                                   '?dataset_name=flu_shot_clinics'
                                   '&obs_date__ge=2013-09-22'
                                   '&obs_date__le=2013-10-1'
                                   '&shape=chicago_neighborhoods')
        self.assertEqual(response.json['meta']['total'], 5)

    def test_bad_column_condition(self):
        ingest_flu_shot_clinics()
        response = self.client.get('v1/api/detail'
                                   '?dataset_name=flu_shot_clinics'
                                   '&fake_column=fake')
        self.assertTrue("Unused parameter value fake_column='fake'" in response.json['meta']['message'])

    def test_bad_column_condition_with_shape(self):
        ingest_flu_shot_clinics()
        response = self.client.get('v1/api/detail'
                                   '?dataset_name=flu_shot_clinics'
                                   '&shape=chicago_neighborhoods'
                                   '&fake_column=fake')
        self.assertTrue("Unused parameter value fake_column='fake'" in response.json['meta']['message'])


# TODO(heyzoos) remove repeated db.session.commit in point api logic
