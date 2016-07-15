import json
import unittest
from plenario import create_app
from plenario.api.validator import Validator
from plenario.utils.model_helpers import table_exists, add_meta_if_not_exists, add_table_if_not_exists
from tests.test_fixtures.post_data import roadworks_post_data


class TestValidator(unittest.TestCase):

    def get_json_response_data(self, endpoint):
        """A little util that does work I found myself repeating alot."""

        response = self.test_client.get('/v1/api/' + endpoint)
        return json.loads(response.data)

    def setUp(self):
        self.app = create_app()
        self.test_client = self.app.test_client()

    def test_validator_bad_dataset_name(self):
        endpoint = 'detail'
        query = '?dataset_name=crimez&obs_date__ge=2000'

        resp_data = self.get_json_response_data(endpoint + query)

        self.assertTrue(len(resp_data['meta']['message']) == 1)
        self.assertEquals(resp_data['meta']['message']['dataset_name'], ['Not a valid choice.'])

    def test_validator_bad_dataset_name_and_date(self):
        endpoint = 'detail'
        query = '?dataset_name=crimez&obs_date__ge=20z00'

        resp_data = self.get_json_response_data(endpoint + query)

        self.assertTrue(len(resp_data['meta']['message']) == 2)
        self.assertEquals(resp_data['meta']['message']['dataset_name'], ['Not a valid choice.'])
        self.assertEquals(resp_data['meta']['message']['obs_date__ge'], ['Not a valid date.'])

    def test_validator_bad_column_name(self):
        endpoint = 'detail'
        query = '?dataset_name=crimes&obs_date__ge=2000&fake_column=fake'

        resp_data = self.get_json_response_data(endpoint + query)

        self.assertTrue(len(resp_data['meta']['message']) == 2)
        self.assertTrue('Unused param' in resp_data['meta']['message'][0])
        self.assertTrue('not a valid column' in resp_data['meta']['message'][1])

    def test_validator_incorrect_datatype(self):
        endpoint = 'detail'
        query = '?dataset_name=crimes&obs_date__ge=2000&data_type=fake'

        resp_data = self.get_json_response_data(endpoint + query)

        self.assertTrue(len(resp_data['meta']['message']) == 1)
        self.assertEquals(resp_data['meta']['message']['data_type'], ['Not a valid choice.'])

    def test_shape_validator_incorrect_datatype(self):
        endpoint = 'shapes/pedestrian_streets'
        query = '?data_type=csv'

        resp_data = self.get_json_response_data(endpoint + query)

        self.assertTrue(len(resp_data['meta']['message']) == 1)
        self.assertEquals(resp_data['meta']['message']['data_type'], ['Not a valid choice.'])

    def test_validator_with_good_column_but_bad_value(self):
        endpoint = 'detail'
        query = '?dataset_name=crimes&data_type=json&id="break_me"'

        resp_data = self.get_json_response_data(endpoint + query)

        self.assertTrue(len(resp_data['meta']['message']) == 2)
        self.assertTrue('Unused param' in resp_data['meta']['message'][0])
        self.assertTrue('not a valid value' in resp_data['meta']['message'][1])

    def test_catches_bad_filter_dataset_name(self):
        endpoint = 'detail'
        query = '?dataset_name=crimes&crimez__filter='
        qfilter = '{"op": "eq", "col": "iucr", "val": 0110}'

        resp_data = self.get_json_response_data(endpoint + query + qfilter)

        self.assertTrue(len(resp_data['meta']['message']) == 1)
        self.assertTrue('Table name crimez could not be found' in resp_data['meta']['message']['crimez'])

    def test_rejects_bad_operator_in_tree(self):
        endpoint = 'detail'
        query = '?dataset_name=crimes&crimes__filter='
        qfilter = '{"op": "eqz", "col": "iucr", "val": 0110}'

        resp_data = self.get_json_response_data(endpoint + query + qfilter)

        self.assertTrue(len(resp_data['meta']['message']) == 1)
        self.assertTrue('causes error' in resp_data['meta']['message']['crimes'])

    def test_rejects_bad_column_in_tree(self):
        endpoint = 'detail'
        query = '?dataset_name=crimes&crimes__filter='
        qfilter = '{"op": "eq", "col": "iucrrr", "val": 0110}'

        resp_data = self.get_json_response_data(endpoint + query + qfilter)

        self.assertTrue(len(resp_data['meta']['message']) == 1)
        self.assertTrue('causes error' in resp_data['meta']['message']['crimes'])

    def test_rejects_bad_value_in_tree(self):
        endpoint = 'detail'
        query = '?dataset_name=crimes&crimes__filter='
        qfilter = '{"op": "eq", "col": "iucr", "val": -0110}'

        resp_data = self.get_json_response_data(endpoint + query + qfilter)

        self.assertTrue(len(resp_data['meta']['message']) == 1)
        self.assertTrue('causes error' in resp_data['meta']['message']['crimes'])

    def test_rejects_empty_tree(self):
        endpoint = 'detail'
        query = '?dataset_name=crimes&crimes__filter='
        qfilter = '{}'

        resp_data = self.get_json_response_data(endpoint + query + qfilter)

        self.assertTrue(len(resp_data['meta']['message']) == 1)
        self.assertTrue('causes error' in resp_data['meta']['message']['crimes'])

    def test_validator_keeps_meta_params_with_a_filter(self):
        endpoint = 'detail'
        query = '?dataset_name=crimes&obs_date__ge=2000&agg=year&crimes__filter='
        qfilter = '{"op": "eq", "col": "iucr", "val": 1150}'

        resp_data = self.get_json_response_data(endpoint + query + qfilter)
        self.assertEqual(len(resp_data['objects']), 2)

    def test_validator_discards_columns_with_a_filter(self):
        endpoint = 'detail-aggregate'
        query = '?dataset_name=crimes&description=CREDIT CARD FRAUD&crimes__filter='
        qfilter = '{"op": "eq", "col": "iucr", "val": 1150}'

        resp_data = self.get_json_response_data(endpoint + query + qfilter)

        self.assertEqual(len(resp_data['meta']['message']), 1)
        self.assertIn('Unused parameter description', resp_data['meta']['message'][0])

    def test_validator_no_table_provided(self):
        endpoint = 'detail-aggregate'
        query = '?description=CREDIT CARD FRAUD'

        resp_data = self.get_json_response_data(endpoint + query)

        self.assertEqual(len(resp_data['meta']['message']), 1)
        self.assertIn('Missing data for required field.', resp_data['meta']['message']['dataset_name'])

    def test_catches_bad_filter_op_keyword(self):
        endpoint = 'detail'
        query = '?dataset_name=crimes&crimes__filter='
        qfilter = '{"opz": "eq"}'

        resp_data = self.get_json_response_data(endpoint + query + qfilter)

        self.assertTrue(len(resp_data['meta']['message']) == 1)
        self.assertIn('Invalid keyword', resp_data['meta']['message']['crimes'])

    def test_catches_bad_filter_col_keyword(self):
        endpoint = 'detail'
        query = '?dataset_name=crimes&crimes__filter='
        qfilter = '{"op": "eq", "colz": "iucr", "val": 1150}'

        resp_data = self.get_json_response_data(endpoint + query + qfilter)

        self.assertTrue(len(resp_data['meta']['message']) == 1)
        self.assertIn('invalid keyword', resp_data['meta']['message']['crimes'])

    def test_catches_bad_filter_val_keyword(self):
        endpoint = 'detail'
        query = '?dataset_name=crimes&crimes__filter='
        qfilter = '{"op": "eq", "col": "iucr", "valz": 1150}'

        resp_data = self.get_json_response_data(endpoint + query + qfilter)

        self.assertTrue(len(resp_data['meta']['message']) == 1)
        self.assertIn('invalid keyword', resp_data['meta']['message']['crimes'])

    def test_updates_index_and_validates_correctly(self):

        validator = Validator()

        add_meta_if_not_exists(app=self.test_client,
                               table_name='roadworks',
                               post_data=roadworks_post_data,
                               shape=False)
        add_table_if_not_exists('roadworks', shape=False)

        validator_result = validator.loads('{"dataset_name": "roadworks"}')
        self.assertFalse(bool(validator_result.errors))
        self.assertEqual('roadworks', validator_result.data['dataset_name'])
