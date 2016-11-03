import subprocess
import unittest
import zipfile

import random
from io import StringIO

from plenario import create_app
from plenario.api import prefix
from plenario.api.jobs import *
from plenario.database import app_engine, session
from plenario.models import MetaTable, ShapeMetadata
from plenario.update import create_worker
from plenario.utils.model_helpers import fetch_table, table_exists
from plenario.utils.shapefile import Shapefile
from plenario.views import approve_dataset, queue_update_dataset, delete_dataset
from plenario.views import approve_shape, queue_update_shape, delete_shape
from tests.test_api.test_point import get_loop_rect


class TestJobs(unittest.TestCase):
    currentTicket = ""

    @staticmethod
    def flush_fixtures():
        sql = "delete from meta_master where dataset_name = 'restaurant_applications'"
        app_engine.execute(sql)
        sql = "delete from meta_shape where dataset_name = 'boundaries_neighborhoods'"
        app_engine.execute(sql)

    @classmethod
    def setUpClass(cls):

        super(TestJobs, cls).setUpClass()

        # FOR THIS TEST TO WORK
        # You need to specify the AWS Keys and the Jobs Queue
        # in the environment variables. On travis, this will be
        # done automatically.

        # setup flask app instance
        cls.app = create_app().test_client()
        cls.other_app = create_app()
        # setup flask app instance for worker
        cls.worker = create_worker().test_client()
        # start worker
        subprocess.Popen(["python", "worker.py"])
        # give worker time to start up
        time.sleep(3)

        # Clearing out the DB.
        try:
            TestJobs.flush_fixtures()
        except:
            pass

        # Seeding the database for ETL Tests.
        restaurants = dict([('col_name_decisiontargetdate', ''), ('col_name_classificationlabel', ''),
                            ('col_name_publicconsultationenddate', ''), ('col_name_locationtext', ''),
                            ('view_url', 'https://opendata.bristol.gov.uk/api/views/5niz-5v5u/rows'), (
                            'dataset_description',
                            'Planning applications details for applications from 2010 to 2014. Locations have been geocoded based on postcode where available.'),
                            ('col_name_decisionnoticedate', ''), ('col_name_casetext', ''),
                            ('update_frequency', 'yearly'), ('col_name_status', ''),
                            ('col_name_location', 'location'), ('col_name_publicconsultationstartdate', ''),
                            ('contributor_email', 'look@me.com'), ('col_name_decision', ''),
                            ('col_name_decisiontype', ''), ('col_name_organisationuri', ''),
                            ('col_name_appealref', ''), ('col_name_coordinatereferencesystem', ''),
                            ('col_name_appealdecision', ''), ('col_name_geoarealabel', ''),
                            ('col_name_organisationlabel', ''), ('contributor_organization', ''),
                            ('col_name_casereference', ''), ('col_name_latitude', ''),
                            ('col_name_servicetypelabel', ''), ('is_shapefile', 'false'),
                            ('col_name_groundarea', ''), ('col_name_postcode', ''), ('col_name_agent', ''),
                            ('col_name_classificationuri', ''), ('col_name_geoy', ''), ('col_name_geox', ''),
                            ('col_name_uprn', ''), ('col_name_geopointlicencingurl', ''),
                            ('col_name_appealdecisiondate', ''), ('col_name_decisiondate', ''),
                            ('col_name_extractdate', 'observed_date'), ('col_name_servicetypeuri', ''),
                            ('col_name_casedate', ''), ('dataset_attribution', 'Bristol City Council'),
                            ('col_name_caseurl', ''), ('contributor_name', 'mrmeseeks'),
                            ('col_name_publisheruri', ''), ('col_name_geoareauri', ''),
                            ('col_name_postcode_sector', ''), ('file_url',
                                                                'https://opendata.bristol.gov.uk/api/views/5niz-5v5u/rows.csv?accessType=DOWNLOAD'),
                            ('col_name_postcode_district', ''), ('col_name_publisherlabel', ''),
                            ('col_name_responsesfor', ''), ('col_name_responsesagainst', ''),
                            ('col_name_longitude', ''), ('dataset_name', 'restaurant_applications')])
        boundaries = dict(
            [('dataset_attribution', 'City of Chicago'), ('contributor_name', 'mrmeseeks'), ('view_url', ''),
             ('file_url', 'https://data.cityofchicago.org/api/geospatial/bbvz-uum9?method=export&format=Shapefile'),
             ('contributor_organization', ''), ('dataset_description',
                                                 'Neighborhood boundaries in Chicago, as developed by the Office of Tourism. These boundaries are approximate and names are not official. The data can be viewed on the Chicago Data Portal with a web browser. However, to view or use the files outside of a web browser, you will need to use compression software and special GIS software, such as ESRI ArcGIS (shapefile) or Google Earth (KML or KMZ), is required.'),
             ('update_frequency', 'yearly'), ('contributor_email', 'look@me.com'), ('is_shapefile', 'true'),
             ('dataset_name', 'boundaries_neighborhoods')])
        cls.app.post('/add?is_shapefile=false', data=restaurants)
        cls.app.post('/add?is_shapefile=true', data=boundaries)

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

    # ============================================================
    # TEST: Admin DB Actions: add, update, delete (meta/shapemeta)
    # ============================================================
    # These tests rely on the 2010_2014_restaurant_applications dataset to
    # be present in the MetaTable and the boundaries_neighborhoods dataset
    # to be present in the ShapeMetadata
    #
    # Links:
    #   https://opendata.bristol.gov.uk/api/views/5niz-5v5u/rows.csv
    #   https://data.cityofchicago.org/api/geospatial/bbvz-uum9?method=export&format=Shapefile

    def admin_test_01_approve_dataset(self):

        # Grab the source url hash.
        dname = 'restaurant_applications'

        # Drop the table if it already exists.
        if table_exists(dname):
            fetch_table(MetaTable).drop()

        q = session.query(MetaTable.source_url_hash)
        source_url_hash = q.filter(MetaTable.dataset_name == dname).scalar()

        # Queue the ingestion job.
        ticket = approve_dataset(source_url_hash)

        wait_on(ticket, 30)
        status = get_status(ticket)['status']

        # First check if it finished correctly.
        self.assertIn(status, ['error', 'success'])
        # Then check if the job was successful.
        self.assertEqual(status, 'success')
        # Now check if the ingestion process ran correctly.
        table = MetaTable.get_by_dataset_name(dname).point_table
        count = len(session.query(table).all())
        self.assertEqual(count, 356)

    def admin_test_02_queue_update_dataset(self):

        # Grab source url hash.
        dname = 'restaurant_applications'
        q = session.query(MetaTable.source_url_hash)
        source_url_hash = q.filter(MetaTable.dataset_name == dname).scalar()

        table = MetaTable.get_by_dataset_name(dname).point_table

        # Queue the update job.
        with self.other_app.test_request_context():
            ticket = queue_update_dataset(source_url_hash).data
            ticket = json.loads(ticket)['ticket']
        wait_on(ticket, 30)
        status = get_status(ticket)['status']
        self.assertIn(status, {'error', 'success'})
        self.assertEqual(status, 'success')
        count = len(session.query(table).all())
        self.assertEqual(count, 356)

    def admin_test_03_delete_dataset(self):

        # Get source url hash.
        dname = 'restaurant_applications'
        q = session.query(MetaTable.source_url_hash)
        source_url_hash = q.filter(MetaTable.dataset_name == dname).scalar()

        table = MetaTable.get_by_dataset_name(dname).point_table
        self.assertTrue(table is not None)

        # Queue the deletion job.
        with self.other_app.test_request_context():
            ticket = delete_dataset(source_url_hash)
            ticket = json.loads(ticket.data)['ticket']
        wait_on(ticket, 30)
        status = get_status(ticket)['status']
        self.assertIn(status, {'error', 'success'})
        self.assertEqual(status, 'success')

        table = MetaTable.get_by_dataset_name(dname)
        self.assertTrue(table is None)

    def admin_test_04_approve_shapeset(self):
        shape_name = 'boundaries_neighborhoods'

        # Drop the table if it already exists.
        if table_exists(shape_name):
            fetch_table(shape_name).drop()

        # Queue the ingestion job.
        with self.other_app.test_request_context():
            ticket = approve_shape(shape_name)

        print(("shape_test.ticket: {}".format(ticket)))

        wait_on(ticket, 30)
        status = get_status(ticket)['status']

        # First check if it finished correctly.
        self.assertIn(status, ['error', 'success'])
        # Then check if the job was successful.
        self.assertEqual(status, 'success')

        # Now check if the ingestion process ran correctly.
        table = ShapeMetadata.get_by_dataset_name(shape_name).shape_table
        count = len(session.query(table).all())
        self.assertEqual(count, 98)

    def admin_test_05_queue_update_shapeset(self):
        # Grab source url hash.
        shape_name = 'boundaries_neighborhoods'

        table = ShapeMetadata.get_by_dataset_name(shape_name).shape_table

        # Queue the update job.
        with self.other_app.test_request_context():
            ticket = queue_update_shape(shape_name).data
            ticket = json.loads(ticket)['ticket']
        wait_on(ticket, 30)
        status = get_status(ticket)['status']
        self.assertIn(status, {'error', 'success'})
        self.assertEqual(status, 'success')
        count = len(session.query(table).all())
        self.assertEqual(count, 98)

    def admin_test_06_update_makes_new_task_if_not_exists(self):

        # Grab the source url hash.
        shape_name = 'boundaries_neighborhoods'

        # Check if the record already exists.
        rp = app_engine.execute("select from etl_task where dataset_name = 'boundaries_neighborhoods'")
        self.assertEqual(len(rp.fetchall()), 1)

        # Delete the ETLTask record.
        app_engine.execute("delete from etl_task where dataset_name = 'boundaries_neighborhoods'")
        rp = app_engine.execute("select from etl_task where dataset_name = 'boundaries_neighborhoods'")
        self.assertEqual(rp.fetchall(), [])

        # Queue the update job.
        with self.other_app.test_request_context():
            ticket = queue_update_shape(shape_name).data
            ticket = json.loads(ticket)['ticket']

        wait_on(ticket, 30)

        # Check that the record was recreated.
        rp = app_engine.execute("select from etl_task where dataset_name = 'boundaries_neighborhoods'")
        self.assertEqual(len(rp.fetchall()), 1)

        status = get_status(ticket)['status']
        self.assertIn(status, {'error', 'success'})
        self.assertEqual(status, 'success')

        table = ShapeMetadata.get_by_dataset_name(shape_name).shape_table
        count = len(session.query(table).all())
        self.assertEqual(count, 98)

    def admin_test_07_delete_shapeset(self):

        # Get source url hash.
        shape_name = 'boundaries_neighborhoods'

        # Queue the deletion job.
        with self.other_app.test_request_context():
            ticket = delete_shape(shape_name)
            ticket = json.loads(ticket.data)['ticket']

        wait_on(ticket, 30)

        status = get_status(ticket)['status']
        self.assertIn(status, {'error', 'success'})
        self.assertEqual(status, 'success')

        table = ShapeMetadata.get_by_dataset_name(shape_name)
        self.assertTrue(table is None)


    # =======================
    # ACCEPTANCE TEST: Job submission
    # =======================

    def test_job_submission_by_api(self):

        # submit job
        # /datasets with a cachebuster at the end
        response = self.app.get(
            prefix + '/datasets?job=true&obs_date__ge=2010-07-08&' + str(random.randrange(0, 1000000)))
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

    # =======================
    # ACCEPTANCE TEST: Get worker statuspage
    # =======================

    def test_worker_statuspage(self):
        # Get page
        response = self.app.get("/workers")
        self.assertEqual(response.status_code, 200)

        # Purge workers
        response = self.app.get("/workers/purge")
        self.assertEqual("class=\"worker" in response.get_data(), False)

    # ============================ ENDPOINT TESTS ============================ #

    # =======================
    # ACCEPTANCE TEST: timeseries
    # =======================

    def test_timeseries_job(self):
        response = self.app.get(
            prefix + '/timeseries/?obs_date__ge=2013-09-22&obs_date__le=2013-10-1&agg=day&job=true&' + str(
                random.randrange(0, 1000000)))
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

        self.assertFalse("error" in list(response.keys()))
        self.assertEqual(response["status"]["status"], "success")
        self.assertEqual(len(response["result"]), 1)
        self.assertEqual(response["result"][0]["source_url"],
                         "https://data.cityofchicago.org/api/views/rfdj-hdmf/rows.csv?accessType=DOWNLOAD")
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
        self.assertFalse("error" in list(response.keys()))
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
        self.assertFalse("error" in list(response.keys()))
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
        self.assertFalse("error" in list(response.keys()))
        self.assertEqual(response["status"]["status"], "success")
        self.assertEqual(len(response["result"]), 1)
        self.assertEqual(response["result"][0]["source_url"],
                         "https://data.cityofchicago.org/api/views/rfdj-hdmf/rows.csv?accessType=DOWNLOAD")
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
        self.assertFalse("error" in list(response.keys()))
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
        self.assertEqual(json.loads(response["request"]["query"]["geom"])["coordinates"],
                         json.loads(query_rect)["geometry"]["coordinates"])
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
        self.assertFalse("error" in list(response.keys()))
        self.assertEqual(response["status"]["status"], "success")
        self.assertEqual(len(response["result"]["features"]), 4)
        self.assertEqual(response["result"]["type"], "FeatureCollection")
        self.assertEqual(response["result"]["features"][0]["properties"]["count"], 1)

    # ===============================
    # ACCEPTANCE TEST: shapes/<shape>
    # ===============================

    def test_export_shape_job(self):

        response = self.app.get("/v1/api/shapes/chicago_neighborhoods?job=true")
        response = json.loads(response.get_data())
        ticket = response['ticket']

        self.assertIsNotNone(ticket)
        self.assertIsNotNone(response["url"])
        self.assertEqual(response["request"]["endpoint"], "export-shape")
        self.assertEqual(response["request"]["query"]["shapeset"], "chicago_neighborhoods")
        self.assertEqual(response["request"]["query"]["job"], True)

        wait_on(ticket, 30)

        url = response["url"]
        response = self.app.get(url)
        response = json.loads(response.get_data())

        self.assertEqual(len(response['features']), 98)

    def test_export_shape_job_shapefile(self):

        response = self.app.get("/v1/api/shapes/chicago_neighborhoods?data_type=shapefile&job=true")
        response = json.loads(response.get_data())
        ticket = response['ticket']

        self.assertIsNotNone(ticket)
        self.assertIsNotNone(response["url"])
        self.assertEqual(response["request"]["endpoint"], "export-shape")
        self.assertEqual(response["request"]["query"]["shapeset"], "chicago_neighborhoods")
        self.assertEqual(response["request"]["query"]["data_type"], "shapefile")
        self.assertEqual(response["request"]["query"]["job"], True)

        wait_on(ticket, 30)

        url = response["url"]
        response = self.app.get(url)

        file_content = StringIO(response.data)
        as_zip = zipfile.ZipFile(file_content)

        # The Shapefile utility class takes a ZipFile, opens it,
        # and throws an exception if it doesn't have the expected shapefile components (.shp and .prj namely)
        with Shapefile(as_zip):
            pass

    def test_aggregate_point_data(self):

        url = '/v1/api/shapes/chicago_neighborhoods/landmarks/' \
              '?obs_date__ge=2000-09-22&obs_date__le=2013-10-1'
        response = self.app.get(url + '&job=true')
        response = json.loads(response.get_data())
        ticket = response['ticket']

        self.assertIsNotNone(ticket)
        self.assertIsNotNone(response["url"])
        self.assertEqual(response["request"]["endpoint"], "aggregate-point-data")
        self.assertEqual(response["request"]["query"]["shapeset"], "chicago_neighborhoods")
        self.assertEqual(response["request"]["query"]["dataset"], "landmarks")
        self.assertEqual(response["request"]["query"]["data_type"], "json")
        self.assertEqual(response["request"]["query"]["job"], True)

        wait_on(ticket, 30)

        url = response["url"]
        response = self.app.get(url)
        data = json.loads(response.data)
        neighborhoods = data['result']['features']
        self.assertEqual(len(neighborhoods), 54)

        for neighborhood in neighborhoods:
            self.assertGreaterEqual(neighborhood['properties']['count'], 1)

    # =======================
    # ACCEPTANCE TEST: datadump (JSON)
    # =======================

    def test_datadump_json_job(self):
        response = self.app.get(
            prefix + '/datadump?obs_date__ge=2000-1-1&obs_date__le=2014-1-1&dataset_name=flu_shot_clinics&' + str(
                random.randrange(0, 1000000)))
        response = json.loads(response.get_data())
        ticket = response["ticket"]
        self.assertIsNotNone(ticket)
        self.assertIsNotNone(response["url"])
        self.assertEqual(response["request"]["endpoint"], "datadump")
        self.assertEqual(response["request"]["query"]["dataset"], "flu_shot_clinics")
        self.assertEqual(response["request"]["query"]["job"], True)

        # Wait for job to complete.
        for i in range(60):
            if get_status(ticket)["status"] == "success":
                break
            time.sleep(1)

        # retrieve job
        url = response["url"]
        response = self.app.get(url)
        response = json.loads(response.get_data())
        self.assertIsNotNone(response["status"]["progress"])

        url = response["result"]["url"]
        response = self.app.get(url)
        response = json.loads(response.get_data())

        self.assertEqual(len(response["data"]), 65)
        self.assertEqual(response["data"][0]["date"], "2013-09-22")
        self.assertIsNotNone(response["startTime"])
        self.assertIsNotNone(response["endTime"])
        self.assertIsNotNone(response["workers"])

    # =======================
    # ACCEPTANCE TEST: datadump (CSV)
    # =======================

    def test_datadump_csv_job(self):
        response = self.app.get(
            prefix + '/datadump?obs_date__ge=2000-1-1&obs_date__le=2014-1-1&dataset_name=flu_shot_clinics&' + str(
                random.randrange(0, 1000000)))
        response = json.loads(response.get_data())
        ticket = response["ticket"]
        self.assertIsNotNone(ticket)
        self.assertIsNotNone(response["url"])
        self.assertEqual(response["request"]["endpoint"], "datadump")
        self.assertEqual(response["request"]["query"]["dataset"], "flu_shot_clinics")
        self.assertEqual(response["request"]["query"]["job"], True)

        # Wait for job to complete.
        for i in range(60):
            if get_status(ticket)["status"] == "success":
                break
            time.sleep(1)

        # retrieve job
        url = response["url"]
        response = self.app.get(url)
        response = json.loads(response.get_data())
        self.assertIsNotNone(response["status"]["progress"])

        url = response["result"]["url"]+"?data_type=csv"
        response = self.app.get(url)

        print((response.get_data()))
        response = response.get_data().split("\n")

        # 65 data lines, 1 column line, and 1 newline at the end.
        self.assertEqual(len(response), 65 + 1 + 1)
        # Uncomment to enable tests for CSV metadata
        # 65 data lines, 4 meta lines, and 1 newline at the end.
        # self.assertEqual(len(response), 65 + 4 + 1)
        # self.assertEqual(response[0][:11], "# STARTTIME")
        # self.assertEqual(response[1][:9], "# ENDTIME")
        # self.assertEqual(response[2][:9], "# WORKERS")
        # self.assertEqual(response[3],
        self.assertEqual(response[0],
                         '"date","start_time","end_time","day","event","event_type","address","city","state","zip","phone","community_area_number","community_area_name","ward","latitude","longitude","location"')
        # Uncomment to enable tests for CSV metadata
        # self.assertEqual(response[4].split(",")[0], '"2013-09-22"')
        self.assertEqual(response[1].split(",")[0], '"2013-09-22"')

    # ============================ TEARDOWN ============================ #

    @classmethod
    def tearDownClass(cls):
        print("Stopping worker.")
        subprocess.Popen(["pkill", "-f", "worker.py"])

        # Clear out the DB.
        try:
            TestJobs.flush_fixtures()
        except:
            pass

        # Release the locks on Postgres.
        session.close()


def wait_on(ticket, seconds):
    if ticket is None:
        return
    for i in range(seconds):
        status = get_status(ticket)['status']
        if status == 'success' or status == 'error':
            break
        time.sleep(1)
