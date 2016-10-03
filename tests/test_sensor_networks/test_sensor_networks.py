import json
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

    def test_network_metadata_returns_bad_request(self):
        url = "/v1/api/sensor-networks/bad_network"
        response = self.app.get(url)
        result = json.loads(response.data)
        self.assertEqual(response.status_code, 400)
        self.assertIn("error", result)

    def test_sensor_metadata_returns_bad_request(self):
        url = "/v1/api/sensor-networks/test_network/sensors/bad_sensor"
        response = self.app.get(url)
        result = json.loads(response.data)
        self.assertEqual(response.status_code, 400)
        self.assertIn("error", result)

    def test_node_metadata_returns_bad_request(self):
        url = "/v1/api/sensor-networks/test_network/nodes/bad_node"
        response = self.app.get(url)
        result = json.loads(response.data)
        self.assertEqual(response.status_code, 400)
        self.assertIn("error", result)

    def test_feature_metadata_returns_bad_request(self):
        url = "/v1/api/sensor-networks/test_network/features_of_interest/bad_feature"
        response = self.app.get(url)
        result = json.loads(response.data)
        self.assertEqual(response.status_code, 400)
        self.assertIn("error", result)

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

    def test_download_queues_job_returns_ticket(self):
        url = "/v1/api/sensor-networks/test_network/download"
        url += "?sensors=test_sensor_01&nodes=test_node&features_of_interest=vector"
        response = self.app.get(url)
        result = json.loads(response.data)
        self.assertIn("ticket", result)

    def test_geom_filter_for_node_metadata(self):
        # Geom box in the middle of the lake, should return no results
        geom = '{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[-86.7315673828125,42.24275208539065],[-86.7315673828125,42.370720143531955],[-86.50360107421875,42.370720143531955],[-86.50360107421875,42.24275208539065],[-86.7315673828125,42.24275208539065]]]}}'
        url = "/v1/api/sensor-networks/test_network/nodes?location_geom__within={}".format(geom)
        response = self.app.get(url)
        result = json.loads(response.data)
        self.assertEqual(result["meta"]["total"], 0)

        # Geom box surrounding chicago, should return results
        geom = '{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[-87.76290893554688,41.7559466348148],[-87.76290893554688,41.95029860413911],[-87.51983642578125,41.95029860413911],[-87.51983642578125,41.7559466348148],[-87.76290893554688,41.7559466348148]]]}}'
        url = "/v1/api/sensor-networks/test_network/nodes?location_geom__within={}".format(geom)
        response = self.app.get(url)
        result = json.loads(response.data)
        self.assertEqual(result["meta"]["total"], 1)

    def test_geom_filter_for_sensor_metadata(self):
        # Geom box in the middle of the lake, should return no results
        geom = '{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[-87.39486694335938,41.823525308461456],[-87.39486694335938,41.879786443521795],[-87.30972290039062,41.879786443521795],[-87.30972290039062,41.823525308461456],[-87.39486694335938,41.823525308461456]]]}}'
        url = "/v1/api/sensor-networks/test_network/sensors?location_geom__within={}".format(geom)
        response = self.app.get(url)
        result = json.loads(response.data)
        self.assertEqual(result["meta"]["total"], 0)

        # Geom box surrounding chicago, should return results
        geom = '{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[-87.76290893554688,41.7559466348148],[-87.76290893554688,41.95029860413911],[-87.51983642578125,41.95029860413911],[-87.51983642578125,41.7559466348148],[-87.76290893554688,41.7559466348148]]]}}'
        url = "/v1/api/sensor-networks/test_network/sensors?location_geom__within={}".format(geom)
        response = self.app.get(url)
        result = json.loads(response.data)
        self.assertEqual(result["meta"]["total"], 2)

    def test_geom_filter_for_feature_metadata(self):
        # Geom box in the middle of the lake, should return no results
        geom = '{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[-87.39486694335938,41.823525308461456],[-87.39486694335938,41.879786443521795],[-87.30972290039062,41.879786443521795],[-87.30972290039062,41.823525308461456],[-87.39486694335938,41.823525308461456]]]}}'
        url = "/v1/api/sensor-networks/test_network/features_of_interest?location_geom__within={}".format(geom)
        response = self.app.get(url)
        result = json.loads(response.data)
        self.assertEqual(result["meta"]["total"], 0)

        # Geom box surrounding chicago, should return results
        geom = '{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[-87.76290893554688,41.7559466348148],[-87.76290893554688,41.95029860413911],[-87.51983642578125,41.95029860413911],[-87.51983642578125,41.7559466348148],[-87.76290893554688,41.7559466348148]]]}}'
        url = "/v1/api/sensor-networks/test_network/features_of_interest?location_geom__within={}".format(geom)
        response = self.app.get(url)
        result = json.loads(response.data)
        self.assertEqual(result["meta"]["total"], 2)

    @classmethod
    def tearDownClass(cls):
        # TODO: Not functioning.
        cls.fixtures.clear_sensor_network_meta_tables()
