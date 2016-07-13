import unittest
import subprocess
import random

from plenario import create_app
from plenario.api import prefix
from plenario.api.jobs import *
from plenario.update import create_worker


class TestJobs(unittest.TestCase):

    currentTicket = ""

    @classmethod
    def setUpClass(cls):

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
        for i in range(10):
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
        for i in range(10):
            if get_status(ticket)["status"] == "success":
                break
            time.sleep(1)

        response = self.app.get(url)
        response = json.loads(response.get_data())
        self.assertIsNotNone(response["status"]["meta"]["startTime"])
        self.assertIsNotNone(response["status"]["meta"]["endTime"])
        self.assertIsNotNone(response["status"]["meta"]["worker"])
        self.assertGreater(json.dumps(response["result"]), len("{\"\"}"))

    # =======================
    # ACCEPTANCE TEST: Get non-existent job
    # =======================

    def test_bad_job_retrieval(self):

        # dummy job with a cachebuster at the end
        ticket = "for_sure_this_isnt_a_job_because_jobs_are_in_hex"
        response = self.app.get(prefix + "/jobs/" + ticket + "&" + str(random.randrange(0, 1000000)))
        response = json.loads(response.get_data())
        self.assertEqual(response["ticket"], ticket)
        self.assertIsNotNone(response["error"])

    # ============================ ENDPOINT TESTS ============================ #

    # =======================
    # ACCEPTANCE TEST: timeseries
    # =======================

    def test_timeseries_job(self):
        response = self.app.get(prefix + '/timeseries/?obs_date__ge=2013-09-22&obs_date__le=2013-10-1&agg=day?job=true&' + str(random.randrange(0, 1000000)))
        response = json.loads(response.get_data())
        ticket = response["ticket"]
        self.assertIsNotNone(ticket)
        self.assertIsNotNone(response["url"])
        self.assertEqual(response["request"]["endpoint"], "timeseries")
        self.assertEqual(response["request"]["query"]["obs_date__ge"], "2013-09-22")
        self.assertEqual(response["request"]["query"]["obs_date__le"], "2013-10-01")
        self.assertEqual(response["request"]["query"]["agg"], "day")
        self.assertEqual(response["request"]["query"]["job"], "true")

        # Wait for job to complete.
        for i in range(10):
            if get_status(ticket)["status"] == "success":
                break
            time.sleep(1)

        # retrieve job
        url = response["url"]
        response = self.app.get(url)
        response = json.loads(response.get_data())
        self.assertIsNone(response["error"])
        self.assertEqual(response["status"]["status"], "success")
        self.assertEqual(response["result"]["meta"]["status"], "ok")
        self.assertEqual(response["result"]["meta"]["query"]["obs_date__ge"], "2013-09-22")
        self.assertEqual(len(response["result"]["objects"]), 1)
        self.assertEqual(response["result"]["objects"]["count"], 5)
        self.assertEqual(response["result"]["objects"][0]["dataset_name"], "flu_shot_clinics")
        

    @classmethod
    def tearDownClass(cls):
        print("Stopping worker.")
        subprocess.Popen(["pkill", "-f", "worker.py"])


