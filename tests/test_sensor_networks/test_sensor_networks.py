import json
import os
import unittest
from fixtures import Fixtures
from plenario import create_app


class TestSensorNetworks(unittest.TestCase):

    fixtures = Fixtures()

    @classmethod
    def setUpClass(cls):
        cls.fixtures.setup_environment()
        cls.app = create_app().test_client()
        cls.fixtures.generate_sensor_network_meta_tables()
        cls.fixtures.generate_mock_metadata()

    def test_network_metadata_returns_200_with_no_args(self):
        url = "/v1/api/sensor-networks/"
        response = self.app.get(url)
        self.assertEqual(response.status_code, 200)
        response = self.app.get(url + "test_network")
        self.assertEqual(response.status_code, 200)

    def test_sensor_metadata_returns_200_with_no_args(self):
        url = "/v1/api/sensor-networks/test_network/sensors"
        response = self.app.get(url)
        self.assertEqual(response.status_code, 200)

    def test_node_metadata_returns_200_with_no_args(self):
        url = "/v1/api/sensor-networks/test_network/nodes"
        response = self.app.get(url)
        self.assertEqual(response.status_code, 200)

    def test_feature_metadata_returns_200_with_no_args(self):
        url = "/v1/api/sensor-networks/test_network/features_of_interest"
        response = self.app.get(url)
        self.assertEqual(response.status_code, 200)

    def test_network_metadata_returns_correct_number_of_results(self):
        url = "/v1/api/sensor-networks"
        response = self.app.get(url)
        response = json.loads(response.data)
        result = response["meta"]["total"]
        self.assertEqual(result, 2)

    def test_node_metadata_returns_correct_number_of_results(self):
        url = "/v1/api/sensor-networks/test_network/nodes"
        response = self.app.get(url)
        response = json.loads(response.data)
        result = response["meta"]["total"]
        self.assertEqual(result, 1)

    def test_sensor_metadata_returns_correct_number_of_results(self):
        url = "/v1/api/sensor-networks/test_network/sensors"
        response = self.app.get(url)
        response = json.loads(response.data)
        result = response["meta"]["total"]
        self.assertEqual(result, 2)

    def test_feature_metadata_returns_correct_number_of_results(self):
        url = "/v1/api/sensor-networks/test_network/features_of_interest"
        response = self.app.get(url)
        response = json.loads(response.data)
        result = response["meta"]["total"]
        self.assertEqual(result, 2)

    @classmethod
    def tearDownClass(cls):
        # TODO: Not functioning.
        cls.fixtures.clear_sensor_network_meta_tables()
