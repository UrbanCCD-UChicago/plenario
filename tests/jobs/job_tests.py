import unittest
import subprocess
import random

from plenario import create_app
from plenario.api import prefix
from plenario.api.jobs import *
from plenario.database import session
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

    # ============================================================
    # TEST: Admin DB Actions: add, update, delete (meta/shapemeta)
    # ============================================================
    # These tests rely on the 2010_2014_restaurant_applications dataset to
    # be present in the MetaTable.
    #
    # Link: https://opendata.bristol.gov.uk/api/views/5niz-5v5u/rows.csv

    def test_admin_dataset_actions(self):
        from sqlalchemy.exc import NoSuchTableError
        from plenario.models import MetaTable
        from plenario.views import approve_dataset, update_dataset
        from plenario.views import delete_dataset

        # Grab the source url hash.
        dname = '2010_2014_restaurant_applications'
        q = session.query(MetaTable.source_url_hash)
        source_url_hash = q.filter(MetaTable.dataset_name == dname).scalar()

        # Queue the ingestion job.
        ticket = approve_dataset(source_url_hash)

        wait_on(ticket, 10)

        status = get_status(ticket)['status']
        # First check if it finished correctly.
        self.assertIn(status, ['error', 'success'])
        # Then check if the job was successful.
        self.assertEqual(status, 'success')
        # Now check if the ingestion process ran correctly.
        table = MetaTable.get_by_dataset_name(dname).point_table
        count = session.query(func.count(table)).first()
        self.assertEqual(count, 317)

        # Queue the update job.
        ticket = update_dataset(source_url_hash).data['ticket']
        wait_on(ticket, 10)
        self.assertIn(status, {'error', 'success'})
        self.assertEqual(status, 'success')
        count = len(session.query(table).all())
        self.assertEqual(count, 317)

        # Queue the deletion job.
        ticket = delete_dataset(source_url_hash).data['ticket']
        wait_on(ticket, 10)
        self.assertIn(status, {'error', 'success'})
        self.assertEqual(status, 'success')
        # Make sure that the table does not exist.
        self.assertRaises(NoSuchTableError, session.query, table)


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
            if response["status"]["status"] == "success":
                break
            time.sleep(1)

        response = self.app.get(url)
        response = json.loads(response.get_data())
        self.assertIsNotNone(response["status"]["meta"]["startTime"])
        self.assertIsNotNone(response["status"]["meta"]["endTime"])
        self.assertGreater(json.dumps(response["result"]), len("{\"\"}"))

    @classmethod
    def tearDownClass(cls):
        print("Stopping worker.")
        subprocess.Popen(["pkill", "-f", "worker.py"])


def wait_on(ticket, seconds):
    for i in range(seconds):
        status = get_status(ticket)['status']
        print "wait_on.status: {}".format(status)
        if status == 'success' or status == 'error':
            break
        time.sleep(1)
