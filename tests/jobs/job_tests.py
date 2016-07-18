import subprocess
import random

from plenario import create_app
from plenario.api import prefix
from plenario.api.jobs import *
from plenario.update import create_worker
from tests.points.api_tests import get_loop_rect
from tests.test_fixtures.base_test import BasePlenarioTest


class TestJobs(BasePlenarioTest):

    currentTicket = ""

    @classmethod
    def setUpClass(cls):

        super(TestJobs, cls).setUpClass()

        # FOR THIS TEST TO WORK
        # You need to specify the AWS Keys and the Jobs Queue
        # in the environment variables. On travis, this will be
        # done automatically.

        # setup flask app instance
        cls.app = create_app().test_client()
        # setup flask app instance for worker
        cls.worker = create_worker().test_client()
        # start worker
        subprocess.Popen(["python", "worker.py"])
        # give worker time to start up
        time.sleep(3)

    # ========================= FUNCTIONALITY TESTS ========================== #

    # =======================
    # TEST: General Job Methods: submit_job, get_status, get_request, get_result
    #       Also test ping endpoint.
    # =======================

    def test_job_submission_by_methods(self):
        ticket = submit_job({"endpoint": "ping", "query": {"test": "abcdefg"}})
        self.assertIsNotNone(ticket)
        status = get_status(ticket)
        self.assertTrue(status["status"] in ["queued", "processing", "success"])
        req = get_request(ticket)
        self.assertEqual(req["endpoint"], "ping")
        self.assertEqual(req["query"]["test"], "abcdefg")

        # Wait for job to complete.
        for i in range(30):
            if get_status(ticket)["status"] == "success":
                break
            time.sleep(1)

        self.assertEqual(get_status(ticket)["status"], "success")
        result = get_result(ticket)
        self.assertIsNotNone(result["hello"])

    # =======================
    # TEST: Job Mutators: set_status, set_request, set_result
    # =======================

    def test_job_mutators(self):
        ticket = "atestticket"

        set_status(ticket, {"status": "funny"})
        self.assertEqual(get_status(ticket), {"status": "funny"})

        set_request(ticket, {"endpoint": "narnia"})
        self.assertEqual(get_request(ticket), {"endpoint": "narnia"})

        set_result(ticket, "the_final_countdown")
        self.assertEqual(get_result(ticket), "the_final_countdown")

    # =======================
    # ACCEPTANCE TEST: Job submission
    # =======================

    def test_job_submission_by_api(self):

        # submit job
        # /datasets with a cachebuster at the end
        response = self.app.get(prefix + '/datasets?job=true&obs_date__ge=2010-07-08&'+str(random.randrange(0,1000000)))
        response = json.loads(response.get_data())
        ticket = response["ticket"]
        self.assertIsNotNone(ticket)
        self.assertIsNotNone(response["url"])
        self.assertEqual(response["request"]["endpoint"], "meta")
        self.assertEqual(response["request"]["query"]["obs_date__ge"], "2010-07-08")

        # retrieve job
        url = response["url"]
        response = self.app.get(url)
        response = json.loads(response.get_data())
        self.assertTrue(response["status"]["status"] in ["queued", "processing", "success"])
        self.assertIsNotNone(response["status"]["meta"]["queueTime"])
        self.assertEqual(response["ticket"], ticket)

        # Wait for job to complete.
        for i in range(30):
            if get_status(ticket)["status"] == "success":
                break
            time.sleep(1)

        response = self.app.get(url)
        response = json.loads(response.get_data())
        self.assertIsNotNone(response["status"]["meta"]["startTime"])
        self.assertIsNotNone(response["status"]["meta"]["endTime"])
        self.assertIsNotNone(response["status"]["meta"]["workers"])
        self.assertGreater(json.dumps(response["result"]), len("{\"\"}"))

    # =======================
    # ACCEPTANCE TEST: Get non-existent job
    # =======================

    def test_bad_job_retrieval(self):

        # dummy job with a cachebuster at the end
        ticket = "for_sure_this_isnt_a_job_because_jobs_are_in_hex"
        response = self.app.get(prefix + "/jobs/" + ticket + "?&" + str(random.randrange(0, 1000000)))
        response = json.loads(response.get_data())
        self.assertEqual(response["ticket"], ticket)
        self.assertIsNotNone(response["error"])

    # ============================ ENDPOINT TESTS ============================ #

    # =======================
    # ACCEPTANCE TEST: timeseries
    # =======================

    def test_timeseries_job(self):
        response = self.app.get(prefix + '/timeseries/?obs_date__ge=2013-09-22&obs_date__le=2013-10-1&agg=day&job=true&' + str(random.randrange(0, 1000000)))
        response = json.loads(response.get_data())
        ticket = response["ticket"]
        self.assertIsNotNone(ticket)
        self.assertIsNotNone(response["url"])
        self.assertEqual(response["request"]["endpoint"], "timeseries")
        self.assertEqual(response["request"]["query"]["obs_date__ge"], "2013-09-22")
        self.assertEqual(response["request"]["query"]["obs_date__le"], "2013-10-01")
        self.assertEqual(response["request"]["query"]["agg"], "day")
        self.assertEqual(response["request"]["query"]["job"], True)

        # Wait for job to complete.
        for i in range(30):
            if get_status(ticket)["status"] == "success":
                break
            time.sleep(1)

        # retrieve job
        url = response["url"]
        response = self.app.get(url)
        response = json.loads(response.get_data())
        self.assertFalse("error" in response.keys())
        self.assertEqual(response["status"]["status"], "success")
        self.assertEqual(len(response["result"]), 1)
        self.assertEqual(response["result"][0]["source_url"], "https://data.cityofchicago.org/api/views/rfdj-hdmf/rows.csv?accessType=DOWNLOAD")
        self.assertEqual(response["result"][0]["count"], 5)
        self.assertEqual(response["result"][0]["dataset_name"], "flu_shot_clinics")

    # =======================
    # ACCEPTANCE TEST: detail-aggregate
    # =======================

    def test_detail_aggregate_job(self):
        response = self.app.get(
            prefix + '/detail-aggregate/?dataset_name=flu_shot_clinics&obs_date__ge=2013-09-22&obs_date__le=2013-10-1&agg=week&job=true&' + str(
                random.randrange(0, 1000000)))
        response = json.loads(response.get_data())
        ticket = response["ticket"]
        self.assertIsNotNone(ticket)
        self.assertIsNotNone(response["url"])
        self.assertEqual(response["request"]["endpoint"], "detail-aggregate")
        self.assertEqual(response["request"]["query"]["obs_date__ge"], "2013-09-22")
        self.assertEqual(response["request"]["query"]["obs_date__le"], "2013-10-01")
        self.assertEqual(response["request"]["query"]["agg"], "week")
        self.assertEqual(response["request"]["query"]["job"], True)

        # Wait for job to complete.
        for i in range(30):
            if get_status(ticket)["status"] == "success":
                break
            time.sleep(1)

        # retrieve job
        url = response["url"]
        response = self.app.get(url)
        response = json.loads(response.get_data())
        self.assertFalse("error" in response.keys())
        self.assertEqual(response["status"]["status"], "success")
        self.assertEqual(response["request"]["query"]["dataset"], "flu_shot_clinics")
        self.assertEqual(len(response["result"]), 3)
        self.assertEqual(response["result"][0]["count"], 1)
        self.assertEqual(response["result"][0]["datetime"], "2013-09-16")

    # =======================
    # ACCEPTANCE TEST: detail
    # =======================

    def test_detail_job(self):
        response = self.app.get(
            prefix + '/detail/?dataset_name=flu_shot_clinics&obs_date__ge=2013-09-22&obs_date__le=2013-10-1&shape=chicago_neighborhoods&job=true&' + str(
                random.randrange(0, 1000000)))
        response = json.loads(response.get_data())
        ticket = response["ticket"]
        self.assertIsNotNone(ticket)
        self.assertIsNotNone(response["url"])
        self.assertEqual(response["request"]["endpoint"], "detail")
        self.assertEqual(response["request"]["query"]["obs_date__ge"], "2013-09-22")
        self.assertEqual(response["request"]["query"]["obs_date__le"], "2013-10-01")
        self.assertEqual(response["request"]["query"]["dataset"], "flu_shot_clinics")
        self.assertEqual(response["request"]["query"]["shapeset"], "chicago_neighborhoods")
        self.assertEqual(response["request"]["query"]["job"], True)

        # Wait for job to complete.
        for i in range(30):
            if get_status(ticket)["status"] == "success":
                break
            time.sleep(1)

        # retrieve job
        url = response["url"]
        response = self.app.get(url)
        response = json.loads(response.get_data())
        self.assertFalse("error" in response.keys())
        self.assertEqual(response["status"]["status"], "success")
        self.assertEqual(len(response["result"]), 5)

    # =======================
    # ACCEPTANCE TEST: meta
    # =======================

    def test_meta_job(self):
        response = self.app.get(
            prefix + '/datasets/?dataset_name=flu_shot_clinics&job=true&' + str(
                random.randrange(0, 1000000)))
        response = json.loads(response.get_data())
        ticket = response["ticket"]
        self.assertIsNotNone(ticket)
        self.assertIsNotNone(response["url"])
        self.assertEqual(response["request"]["endpoint"], "meta")
        self.assertEqual(response["request"]["query"]["dataset"], "flu_shot_clinics")
        self.assertEqual(response["request"]["query"]["job"], True)

        # Wait for job to complete.
        for i in range(30):
            if get_status(ticket)["status"] == "success":
                break
            time.sleep(1)

        # retrieve job
        url = response["url"]
        response = self.app.get(url)
        response = json.loads(response.get_data())
        self.assertFalse("error" in response.keys())
        self.assertEqual(response["status"]["status"], "success")
        self.assertEqual(len(response["result"]), 1)
        self.assertEqual(response["result"][0]["source_url"], "https://data.cityofchicago.org/api/views/rfdj-hdmf/rows.csv?accessType=DOWNLOAD")
        self.assertEqual(response["result"][0]["human_name"], "Flu Shot Clinic Locations")
        self.assertEqual(len(response["result"][0]["columns"]), 17)

    # =======================
    # ACCEPTANCE TEST: fields
    # =======================

    def test_fields_job(self):
        response = self.app.get(
            prefix + '/fields/flu_shot_clinics?job=true&' + str(
                random.randrange(0, 1000000)))
        response = json.loads(response.get_data())
        ticket = response["ticket"]
        self.assertIsNotNone(ticket)
        self.assertIsNotNone(response["url"])
        self.assertEqual(response["request"]["endpoint"], "fields")
        self.assertEqual(response["request"]["query"]["job"], True)

        # Wait for job to complete.
        for i in range(30):
            if get_status(ticket)["status"] == "success":
                break
            time.sleep(1)

        # retrieve job
        url = response["url"]
        response = self.app.get(url)
        response = json.loads(response.get_data())
        self.assertFalse("error" in response.keys())
        self.assertEqual(response["status"]["status"], "success")
        self.assertEqual(len(response["result"]), 1)
        self.assertEqual(len(response["result"][0]["columns"]), 17)
        self.assertEqual(response["result"][0]["columns"][0]["field_type"], "VARCHAR")
        self.assertEqual(response["result"][0]["columns"][0]["field_name"], "city")

    # =======================
    # ACCEPTANCE TEST: grid
    # =======================

    def test_grid_job(self):

        # Get location geom string (non-escaped) for verification
        import os
        pwd = os.path.dirname(os.path.realpath(__file__))
        rect_path = os.path.join(pwd, '../test_fixtures/loop_rectangle.json')
        with open(rect_path, 'r') as rect_json:
            query_rect = rect_json.read()

        response = self.app.get(
            prefix + '/grid/?obs_date__ge=2013-1-1&obs_date__le=2014-1-1&dataset_name=flu_shot_clinics&location_geom__within=' + get_loop_rect() + '&job=true&' + str(
                random.randrange(0, 1000000)))
        response = json.loads(response.get_data())
        ticket = response["ticket"]
        self.assertIsNotNone(ticket)
        self.assertIsNotNone(response["url"])
        self.assertEqual(response["request"]["endpoint"], "grid")
        self.assertEqual(response["request"]["query"]["dataset"], "flu_shot_clinics")
        self.assertEqual(response["request"]["query"]["obs_date__ge"], "2013-01-01")
        self.assertEqual(response["request"]["query"]["obs_date__le"], "2014-01-01")
        self.assertEqual(json.loads(response["request"]["query"]["geom"])["coordinates"], json.loads(query_rect)["geometry"]["coordinates"])
        self.assertEqual(response["request"]["query"]["job"], True)

        # Wait for job to complete.
        for i in range(30):
            if get_status(ticket)["status"] == "success":
                break
            time.sleep(1)

        # retrieve job
        url = response["url"]
        response = self.app.get(url)
        response = json.loads(response.get_data())
        self.assertFalse("error" in response.keys())
        self.assertEqual(response["status"]["status"], "success")
        self.assertEqual(len(response["result"]["features"]), 4)
        self.assertEqual(response["result"]["type"], "FeatureCollection")
        self.assertEqual(response["result"]["features"][0]["properties"]["count"], 1)

    # =======================
    # ACCEPTANCE TEST: datadump (JSON)
    # =======================

    def test_datadump_json_job(self):
        response = self.app.get(prefix + '/datadump?obs_date__ge=2000-1-1&obs_date__le=2014-1-1&dataset_name=flu_shot_clinics')
        try:
            response = json.loads(response.get_data())
        except Exception as e:
            self.fail("Response is not valid JSON (it probably failed): {}".format(e))
        self.assertEqual(len(response["data"]), 65)
        self.assertEqual(response["data"][0]["date"], "2013-12-14")
        self.assertIsNotNone(response["startTime"])
        self.assertIsNotNone(response["endTime"])
        self.assertIsNotNone(response["workers"])

    # =======================
    # ACCEPTANCE TEST: datadump (CSV)
    # =======================

    def test_datadump_csv_job(self):
        response = self.app.get(
            prefix + '/datadump?obs_date__ge=2000-1-1&obs_date__le=2014-1-1&dataset_name=flu_shot_clinics')
        try:
            response = json.loads(response.get_data())
        except Exception as e:
            self.fail("Response is not valid JSON (it probably failed): {}".format(e))
        self.assertEqual(len(response["data"]), 65)
        self.assertEqual(response["data"][0]["date"], "2013-12-14")
        self.assertIsNotNone(response["startTime"])
        self.assertIsNotNone(response["endTime"])
        self.assertIsNotNone(response["workers"])

    # ============================ TEARDOWN ============================ #

    @classmethod
    def tearDownClass(cls):
        print("Stopping worker.")
        subprocess.Popen(["pkill", "-f", "worker.py"])




