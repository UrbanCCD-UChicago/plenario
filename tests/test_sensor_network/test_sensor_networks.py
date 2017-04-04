import json
import time
import unittest

from .fixtures import Fixtures


class TestSensorNetworks(unittest.TestCase):

    fixtures = Fixtures()

    def get_result(self, url):
        response = self.app.get(url)
        return response, json.loads(response.data.decode("utf-8"))

    @classmethod
    def setUpClass(cls):
        cls.fixtures.drop_databases()
        cls.fixtures.setup_databases()
        cls.fixtures.generate_sensor_network_meta_tables()
        cls.fixtures.generate_mock_observations()
        cls.fixtures.generate_mock_metadata()
        cls.app = create_app().test_client()

    def test_network_metadata_with_no_args(self):
        response, data = self.get_result("/v1/api/sensor-networks")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data["meta"]["total"], 2)

    def test_network_metadata_returns_bad_request(self):
        response, data = self.get_result("/v1/api/sensor-networks/bad_network")
        self.assertEqual(response.status_code, 400)
        self.assertIn("error", data)

    def test_network_metadata_case_insensitive(self):
        response, data = self.get_result("/v1/api/sensor-networks/TEST_network")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data["meta"]["total"], 1)

    def test_node_metadata_with_no_args(self):
        response, data = self.get_result("/v1/api/sensor-networks/test_network/"
                                         "nodes")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data["meta"]["total"], 2)

    def test_node_metadata_returns_bad_request(self):
        response, data = self.get_result("/v1/api/sensor-networks/test_network/"
                                         "nodes/bad_node")
        self.assertEqual(response.status_code, 400)
        self.assertIn("error", data)

    def test_node_metadata_with_arg(self):
        response, data = self.get_result("/v1/api/sensor-networks/test_network/"
                                         "nodes/test_node")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data["meta"]["total"], 1)

    def test_node_metadata_case_insensitive(self):
        response, data = self.get_result("/v1/api/sensor-networks/test_network/"
                                         "nodes/TEST_node")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data["meta"]["total"], 1)

    def test_sensor_metadata_returns_with_no_args(self):
        response, _ = self.get_result("/v1/api/sensor-networks/test_network/"
                                      "sensors")
        self.assertEqual(response.status_code, 200)

    def test_feature_metadata_returns_200_with_no_args(self):
        response, _ = self.get_result("/v1/api/sensor-networks/test_network/"
                                      "features")
        self.assertEqual(response.status_code, 200)

    def test_sensor_metadata_returns_bad_request(self):
        response, data = self.get_result("/v1/api/sensor-networks/test_network/"
                                         "sensors/bad_sensor")
        self.assertEqual(response.status_code, 400)
        self.assertIn("error", data)

    def test_feature_metadata_returns_bad_request(self):
        response, data = self.get_result("/v1/api/sensor-networks/test_network/"
                                         "features/bad_feature")
        self.assertEqual(response.status_code, 400)
        self.assertIn("error", data)

    def test_sensor_metadata_returns_correct_number_of_results(self):
        _, data = self.get_result("/v1/api/sensor-networks/test_network/sensors")
        self.assertEqual(data["meta"]["total"], 3)

    def test_feature_metadata_returns_correct_number_of_results(self):
        _, data = self.get_result("/v1/api/sensor-networks/test_network/features")
        self.assertEqual(data["meta"]["total"], 2)

    def test_geom_filter_for_node_metadata_empty_filter(self):
        # Geom box in the middle of the lake, should return no results
        geom = '{"type":"Feature","properties":{},"geometry":' \
               '{"type":"Polygon","coordinates":[[' \
               '[-86.7315673828125,42.24275208539065],' \
               '[-86.7315673828125,42.370720143531955],' \
               '[-86.50360107421875,42.370720143531955],' \
               '[-86.50360107421875,42.24275208539065],' \
               '[-86.7315673828125,42.24275208539065]' \
               ']]}}'
        url = "/v1/api/sensor-networks/test_network/nodes?geom={}".format(geom)
        _, result = self.get_result(url)
        self.assertIn("error", result)

    def test_geom_filter_for_node_metadata_bad_filter(self):
        # Malformed filter
        geom = '{"type":"Feature","properties":{},"geometry":' \
               '{"type":"Polygon","coordinates":[[' \
               '[Why hello!],' \
               '[-86.7315673828125,42.370720143531955],' \
               '[-86.50360107421875,42.370720143531955],' \
               '[-86.50360107421875,42.24275208539065],' \
               '[-86.7315673828125,42.24275208539065]' \
               ']]}}'
        url = "/v1/api/sensor-networks/test_network/nodes?geom={}".format(geom)
        response, result = self.get_result(url)
        self.assertEqual(response.status_code, 400)
        self.assertIn("error", result)

    def test_geom_filter_for_node_metadata_good_filter(self):
        # Geom box surrounding chicago, should return results
        geom = '{"type":"Feature","properties":{},"geometry":' \
               '{"type":"Polygon","coordinates":[[' \
               '[-87.747802734375,41.75799552006108],' \
               '[-87.747802734375,41.93088998442502],' \
               '[-87.5555419921875,41.93088998442502],' \
               '[-87.5555419921875,41.75799552006108],' \
               '[-87.747802734375,41.75799552006108]' \
               ']]}}'
        url = "/v1/api/sensor-networks/test_network/nodes?geom={}"
        url = url.format(geom)
        response, result = self.get_result(url)
        self.assertEqual(result["meta"]["total"], 2)

    def test_geom_filter_for_sensor_metadata(self):
        # Geom box in the middle of the lake, should return no results
        geom = '{"type":"Feature","properties":{},"geometry":' \
               '{"type":"Polygon","coordinates":[[' \
               '[-87.39486694335938,41.823525308461456],' \
               '[-87.39486694335938,41.879786443521795],' \
               '[-87.30972290039062,41.879786443521795],' \
               '[-87.30972290039062,41.823525308461456],' \
               '[-87.39486694335938,41.823525308461456]' \
               ']]}}'
        url = "/v1/api/sensor-networks/test_network/sensors?geom={}"
        url = url.format(geom)
        response, result = self.get_result(url)
        self.assertIn("error", result)

        # Geom box surrounding chicago, should return results
        geom = '{"type":"Feature","properties":{},"geometry":' \
               '{"type":"Polygon","coordinates":[[' \
               '[-87.76290893554688,41.7559466348148],' \
               '[-87.76290893554688,41.95029860413911],' \
               '[-87.51983642578125,41.95029860413911],' \
               '[-87.51983642578125,41.7559466348148],' \
               '[-87.76290893554688,41.7559466348148]' \
               ']]}}'
        url = "/v1/api/sensor-networks/test_network/sensors?geom={}"
        url = url.format(geom)
        response, result = self.get_result(url)
        self.assertEqual(result["meta"]["total"], 3)

    def test_geom_filter_for_feature_metadata(self):
        # Geom box in the middle of the lake, should return no results
        geom = '{"type":"Feature","properties":{},"geometry":' \
               '{"type":"Polygon","coordinates":[[' \
               '[-87.37770080566405,41.95131994679697],' \
               '[-87.37770080566405,41.96357478222518],' \
               '[-87.36328125,41.96357478222518],' \
               '[-87.36328125,41.95131994679697],' \
               '[-87.37770080566405,41.95131994679697]' \
               ']]}}'
        url = "/v1/api/sensor-networks/test_network/features?geom={}"
        url = url.format(geom)
        response, result = self.get_result(url)
        self.assertIn("error", result)

        # Geom box surrounding chicago, should return results
        geom = '{"type":"Feature","properties":{},"geometry":' \
               '{"type":"Polygon","coordinates":[[' \
               '[-87.76290893554688,41.7559466348148],' \
               '[-87.76290893554688,41.95029860413911],' \
               '[-87.51983642578125,41.95029860413911],' \
               '[-87.51983642578125,41.7559466348148],' \
               '[-87.76290893554688,41.7559466348148]' \
               ']]}}'
        url = "/v1/api/sensor-networks/test_network/features?geom={}"
        url = url.format(geom)
        response, result = self.get_result(url)
        self.assertEqual(result["meta"]["total"], 2)

    def test_aggregate_endpoint_returns_correct_bucket_count(self):
        url = "/v1/api/sensor-networks/test_network/aggregate?node=test_node"
        url += "&function=avg&feature=vector"
        url += "&start_datetime=2016-10-01&end_datetime=2016-10-03"
        response, result = self.get_result(url)
        self.assertEqual(result["meta"]["total"], 48)

    def test_aggregate_endpoint_returns_correct_observation_count(self):
        url = "/v1/api/sensor-networks/test_network/aggregate?node=test_node"
        url += "&function=avg&feature=vector.x"
        url += "&start_datetime=2016-10-01&end_datetime=2016-10-03"
        response, result = self.get_result(url)
        total_count = 0
        for bucket in result["data"]:
            for item in bucket.values():
                try:
                    total_count += item["count"]
                except TypeError:
                    pass
        self.assertEqual(total_count, 200)

    def test_aggregate_endpoint_returns_correct_observation_count_with_sensor_filter(self):
        url = "/v1/api/sensor-networks/test_network/aggregate?node=test_node"
        url += "&function=avg&feature=temperature&sensors=sensor_03"
        url += "&start_datetime=2016-10-01&end_datetime=2016-10-03"
        response, result = self.get_result(url)
        total_count = 0
        for bucket in result["data"]:
            for item in bucket.values():
                try:
                    total_count += item["count"]
                except TypeError:
                    pass
        self.assertEqual(total_count, 200)

    def test_query_endpoint_returns_correct_observation_count_total(self):
        url = "/v1/api/sensor-networks/test_network/query?nodes=test_node"
        url += "&feature=vector&start_datetime=2016-01-01"
        response, result = self.get_result(url)
        self.assertEqual(result["meta"]["total"], 300)

    def test_query_endpoint_returns_correct_observation_count_windowed(self):
        url = "/v1/api/sensor-networks/test_network/query?nodes=test_node"
        url += "&feature=vector&start_datetime=2016-10-01"
        url += "&end_datetime=2016-10-03"
        response, result = self.get_result(url)
        self.assertEqual(result["meta"]["total"], 200)

    def test_sensor_metadata_case_insensitive(self):
        url = "/v1/api/sensor-networks/test_network/sensors/SENSor_01"
        response, result = self.get_result(url)
        self.assertEqual(result["meta"]["total"], 1)

    def test_feature_metadata_case_insensitive(self):
        url = "/v1/api/sensor-networks/test_network/features/VECtor"
        response, result = self.get_result(url)
        self.assertEqual(result["meta"]["total"], 1)

    def test_sensor_network_download_csv(self):
        url = "/v1/api/sensor-networks/test_network/download?start_datetime=2016-10-01T00:00:00"
        response = self.app.get(url)

        # 900 rows and 2 headers (because there's two features: temperature and vector)
        expected_number_of_rows = 902
        received_rows = response.get_data().split(b'\r\n')
        received_rows_without_blank_lines = [e for e in received_rows if e]
        received_number_of_rows = len(received_rows_without_blank_lines)
        self.assertEqual(expected_number_of_rows, received_number_of_rows)

    def test_sensor_network_download_with_node_and_feature_args(self):
        url = "/v1/api/sensor-networks/test_network/download?nodes=test_node&" \
              "features=vector&start_datetime=2016-10-01&end_datetime=2016-10-02"
        response = self.app.get(url)

        expected_number_of_rows = 101
        received_rows = response.get_data().split(b'\r\n')
        received_rows_without_blank_lines = [e for e in received_rows if e]
        received_number_of_rows = len(received_rows_without_blank_lines)
        self.assertEqual(expected_number_of_rows, received_number_of_rows)

    def test_sensor_network_download_json(self):
        url = "/v1/api/sensor-networks/test_network/download?start_datetime=2016-10-01T00:00:00&data_type=json"
        response = self.app.get(url)

        expected_number_of_objects = 900
        received_number_of_objects = len(json.loads(response.get_data().decode('utf-8'))['objects'])
        self.assertEqual(expected_number_of_objects, received_number_of_objects)

    def test_sensor_network_download_csv_with_feature_filter(self):
        url = "/v1/api/sensor-networks/test_network/download?" \
              "start_datetime=2016-10-01T00:00:00&"            \
              "feature=temperature"

        response = self.app.get(url)

        # 600 rows and 1 header (temperature)
        expected_number_of_rows = 601
        received_rows = response.get_data().split(b'\r\n')
        received_rows_without_blank_lines = [e for e in received_rows if e]
        received_number_of_rows = len(received_rows_without_blank_lines)
        self.assertEqual(expected_number_of_rows, received_number_of_rows)

    def test_sensor_network_download_json_with_feature_filter(self):
        url = "/v1/api/sensor-networks/test_network/download?" \
              "start_datetime=2016-10-01T00:00:00&"            \
              "data_type=json&"                                \
              "feature=vector"

        response = self.app.get(url)

        expected_number_of_objects = 300
        received_number_of_objects = len(json.loads(response.get_data().decode('utf-8'))['objects'])
        self.assertEqual(expected_number_of_objects, received_number_of_objects)

    def test_sensor_network_download_csv_with_feature_and_sensor_filter(self):
        url = "/v1/api/sensor-networks/test_network/download?" \
              "start_datetime=2016-10-01T00:00:00&"            \
              "feature=temperature&"                           \
              "sensor=sensor_01"

        response = self.app.get(url)

        # 300 rows and 1 header (temperature)
        expected_number_of_rows = 301
        received_rows = response.get_data().split(b'\r\n')
        received_rows_without_blank_lines = [e for e in received_rows if e]
        received_number_of_rows = len(received_rows_without_blank_lines)
        self.assertEqual(expected_number_of_rows, received_number_of_rows)

from plenario import create_app
