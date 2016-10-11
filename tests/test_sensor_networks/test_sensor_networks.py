import json
import time
import unittest
from datetime import datetime, timedelta
from fixtures import Fixtures
from plenario import create_app


class TestSensorNetworks(unittest.TestCase):

    fixtures = Fixtures()

    @classmethod
    def setUpClass(cls):
        cls.fixtures.drop_databases()
        cls.fixtures.setup_databases()
        cls.fixtures.generate_sensor_network_meta_tables()
        cls.fixtures.generate_mock_observations()
        cls.fixtures.generate_mock_metadata()
        cls.fixtures.run_worker()
        cls.app = create_app().test_client()

    def test_network_metadata_with_no_args(self):
        url = "/v1/api/sensor-networks"
        response = self.app.get(url)
        result = json.loads(response.data)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(result["meta"]["total"], 2)

    def test_network_metadata_returns_bad_request(self):
        url = "/v1/api/sensor-networks/bad_network"
        response = self.app.get(url)
        result = json.loads(response.data)
        self.assertEqual(response.status_code, 400)
        self.assertIn("error", result)

    def test_network_metadata_case_insensitive(self):
        url = "/v1/api/sensor-networks/TEST_network"
        response = self.app.get(url)
        result = json.loads(response.data)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(result["meta"]["total"], 1)

    def test_node_metadata_with_no_args(self):
        url = "/v1/api/sensor-networks/test_network/nodes"
        response = self.app.get(url)
        result = json.loads(response.data)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(result["meta"]["total"], 2)

    def test_node_metadata_returns_bad_request(self):
        url = "/v1/api/sensor-networks/test_network/nodes/bad_node"
        response = self.app.get(url)
        result = json.loads(response.data)
        self.assertEqual(response.status_code, 400)
        self.assertIn("error", result)

    def test_node_metadata_with_arg(self):
        url = "/v1/api/sensor-networks/test_network/nodes/test_node"
        response = self.app.get(url)
        result = json.loads(response.data)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(result["meta"]["total"], 1)

    def test_node_metadata_case_insensitive(self):
        url = "/v1/api/sensor-networks/test_network/nodes/TEST_node"
        response = self.app.get(url)
        result = json.loads(response.data)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(result["meta"]["total"], 1)

    def test_sensor_metadata_returns_with_no_args(self):
        url = "/v1/api/sensor-networks/test_network/sensors"
        response = self.app.get(url)
        self.assertEqual(response.status_code, 200)

    def test_feature_metadata_returns_200_with_no_args(self):
        url = "/v1/api/sensor-networks/test_network/features"
        response = self.app.get(url)
        self.assertEqual(response.status_code, 200)

    def test_sensor_metadata_returns_bad_request(self):
        url = "/v1/api/sensor-networks/test_network/sensors/bad_sensor"
        response = self.app.get(url)
        result = json.loads(response.data)
        self.assertEqual(response.status_code, 400)
        self.assertIn("error", result)

    def test_feature_metadata_returns_bad_request(self):
        url = "/v1/api/sensor-networks/test_network/features/bad_feature"
        response = self.app.get(url)
        result = json.loads(response.data)
        self.assertEqual(response.status_code, 400)
        self.assertIn("error", result)

    def test_sensor_metadata_returns_correct_number_of_results(self):
        url = "/v1/api/sensor-networks/test_network/sensors"
        response = self.app.get(url)
        response = json.loads(response.data)
        result = response["meta"]["total"]
        self.assertEqual(result, 2)

    def test_feature_metadata_returns_correct_number_of_results(self):
        url = "/v1/api/sensor-networks/test_network/features"
        response = self.app.get(url)
        response = json.loads(response.data)
        result = response["meta"]["total"]
        self.assertEqual(result, 2)

    def test_download_queues_job_returns_ticket(self):
        url = "/v1/api/sensor-networks/test_network/download"
        url += "?sensors=sensor_01&nodes=test_node&features=vector"
        response = self.app.get(url)
        result = json.loads(response.data)
        self.assertIn("ticket", result)

    # def test_download_queues_job_returns_error_for_bad_args(self):
    #     queueing_url = "/v1/api/sensor-networks/test_network/download"
    #     queueing_url += "?sensors=sensor_01&nodes=test_node&features_of_interest=vector"
    #     queueing_response = self.app.get(queueing_url)
    #
    #     ticket = json.loads(queueing_response.data)["ticket"]
    #     ticket_url = "v1/api/jobs/{}".format(ticket)
    #     ticket_response = self.app.get(ticket_url)
    #     ticket_result = json.loads(ticket_response.data)
    #     self.assertIn("error", ticket_result["result"])

    def test_download_queues_job_returns_correct_result_for_good_args(self):
        queueing_url = "/v1/api/sensor-networks/test_network/download"
        queueing_url += "?sensors=sensor_01&nodes=test_node&features=temperature"
        queueing_response = self.app.get(queueing_url)

        ticket = json.loads(queueing_response.data)["ticket"]
        ticket_url = "v1/api/jobs/{}".format(ticket)
        ticket_response = self.app.get(ticket_url)
        ticket_result = json.loads(ticket_response.data)

        while ticket_result["status"]["status"] not in {"error", "success"}:
            ticket_response = self.app.get(ticket_url)
            ticket_result = json.loads(ticket_response.data)
            time.sleep(1)

        download_url = ticket_result["result"]["url"]
        download_response = self.app.get(download_url)
        download_result = json.loads(download_response.data)
        self.assertEqual(len(download_result["data"]), 300)

    def test_geom_filter_for_node_metadata_empty_filter(self):
        # Geom box in the middle of the lake, should return no results
        geom = '{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[-86.7315673828125,42.24275208539065],[-86.7315673828125,42.370720143531955],[-86.50360107421875,42.370720143531955],[-86.50360107421875,42.24275208539065],[-86.7315673828125,42.24275208539065]]]}}'
        url = "/v1/api/sensor-networks/test_network/nodes?geom={}".format(geom)
        response = self.app.get(url)
        result = json.loads(response.data)
        self.assertIn("error", result)

    def test_geom_filter_for_node_metadata_bad_filter(self):
        # Geom box in the middle of the lake, returns no results (I hope)
        geom = '{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[Why hello!],[-86.7315673828125,42.370720143531955],[-86.50360107421875,42.370720143531955],[-86.50360107421875,42.24275208539065],[-86.7315673828125,42.24275208539065]]]}}'
        url = "/v1/api/sensor-networks/test_network/nodes?geom={}".format(geom)
        response = self.app.get(url)
        result = json.loads(response.data)
        self.assertEqual(response.status_code, 400)
        self.assertIn("error", result)

    def test_geom_filter_for_node_metadata_good_filter(self):
        # Geom box surrounding chicago, should return results
        geom = '{"type"%3A"Feature"%2C"properties"%3A{}%2C"geometry"%3A{"type"%3A"Polygon"%2C"coordinates"%3A[[[-87.747802734375%2C41.75799552006108]%2C[-87.747802734375%2C41.93088998442502]%2C[-87.5555419921875%2C41.93088998442502]%2C[-87.5555419921875%2C41.75799552006108]%2C[-87.747802734375%2C41.75799552006108]]]}}'
        url = "/v1/api/sensor-networks/test_network/nodes?geom={}".format(geom)
        response = self.app.get(url)
        result = json.loads(response.data)
        self.assertEqual(result["meta"]["total"], 2)

    def test_geom_filter_for_sensor_metadata(self):
        # Geom box in the middle of the lake, should return no results
        geom = '{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[-87.39486694335938,41.823525308461456],[-87.39486694335938,41.879786443521795],[-87.30972290039062,41.879786443521795],[-87.30972290039062,41.823525308461456],[-87.39486694335938,41.823525308461456]]]}}'
        url = "/v1/api/sensor-networks/test_network/sensors?geom={}".format(geom)
        response = self.app.get(url)
        result = json.loads(response.data)
        self.assertIn("error", result)

        # Geom box surrounding chicago, should return results
        geom = '{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[-87.76290893554688,41.7559466348148],[-87.76290893554688,41.95029860413911],[-87.51983642578125,41.95029860413911],[-87.51983642578125,41.7559466348148],[-87.76290893554688,41.7559466348148]]]}}'
        url = "/v1/api/sensor-networks/test_network/sensors?geom={}".format(geom)
        response = self.app.get(url)
        result = json.loads(response.data)
        self.assertEqual(result["meta"]["total"], 2)

    def test_geom_filter_for_feature_metadata(self):
        # Geom box in the middle of the lake, should return no results
        geom = '{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[-87.37770080566405,41.95131994679697],[-87.37770080566405,41.96357478222518],[-87.36328125,41.96357478222518],[-87.36328125,41.95131994679697],[-87.37770080566405,41.95131994679697]]]}}'
        url = "/v1/api/sensor-networks/test_network/features?geom={}".format(geom)
        response = self.app.get(url)
        result = json.loads(response.data)
        self.assertIn("error", result)

        # Geom box surrounding chicago, should return results
        geom = '{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[-87.76290893554688,41.7559466348148],[-87.76290893554688,41.95029860413911],[-87.51983642578125,41.95029860413911],[-87.51983642578125,41.7559466348148],[-87.76290893554688,41.7559466348148]]]}}'
        url = "/v1/api/sensor-networks/test_network/features?geom={}".format(geom)
        response = self.app.get(url)
        result = json.loads(response.data)
        self.assertEqual(result["meta"]["total"], 2)

    def test_aggregate_endpoint_returns_correct_bucket_count(self):
        url = "/v1/api/sensor-networks/test_network/aggregate?node=test_node"
        url += "&function=avg&feature=vector"
        url += "&start_datetime=2016-10-01&end_datetime=2016-10-03"
        response = self.app.get(url)
        result = json.loads(response.data)
        self.assertEqual(result["meta"]["total"], 48)

    def test_aggregate_endpoint_returns_correct_observation_count(self):
        url = "/v1/api/sensor-networks/test_network/aggregate?node=test_node"
        url += "&function=avg&feature=vector"
        url += "&start_datetime=2016-10-01&end_datetime=2016-10-03"
        response = self.app.get(url)
        result = json.loads(response.data)
        total_count = 0
        for bucket in result["data"]:
            total_count += bucket.values()[0]["count"]
        self.assertEqual(total_count, 200)

    def test_query_endpoint_returns_correct_observation_count_total(self):
        url = "/v1/api/sensor-networks/test_network/query?nodes=test_node"
        url += "&feature=vector&start_datetime=2016-01-01"
        response = self.app.get(url)
        result = json.loads(response.data)
        self.assertEqual(result["meta"]["total"], 300)

    def test_query_endpoint_returns_correct_observation_count_windowed(self):
        url = "/v1/api/sensor-networks/test_network/query?nodes=test_node"
        url += "&feature=vector&start_datetime=2016-10-01&end_datetime=2016-10-03"
        response = self.app.get(url)
        result = json.loads(response.data)
        self.assertEqual(result["meta"]["total"], 200)

    def test_sensor_metadata_case_insensitive(self):
        url = "/v1/api/sensor-networks/test_network/sensors/SENSor_01"
        response = self.app.get(url)
        result = json.loads(response.data)
        self.assertEqual(result["meta"]["total"], 1)

    def test_feature_metadata_case_insensitive(self):
        url = "/v1/api/sensor-networks/test_network/features/VECtor"
        response = self.app.get(url)
        result = json.loads(response.data)
        self.assertEqual(result["meta"]["total"], 1)

    def test_validator_rejects_datetimes_too_close_to_current(self):
        too_close = datetime.utcnow() - timedelta(minutes=30)
        url = "/v1/api/sensor-networks/test_network/query?nodes=test_node"
        url += "&feature=vector&start_datetime=2016-10-01&end_datetime={}".format(too_close)
        response = self.app.get(url)
        self.assertEqual(response.status_code, 400)

    # todo
    # -----------------------------------------------
    # def alter second node location for better tests

    @classmethod
    def tearDownClass(cls):
        cls.fixtures.kill_worker()
