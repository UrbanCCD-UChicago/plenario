import unittest
import time

from sqlalchemy.exc import IntegrityError

from plenario import create_app
from plenario.database import session
from plenario.models_.ETLTask import add_task, fetch_task, delete_task
from plenario.models_.ETLTask import update_task, fetch_table_etl_status
from plenario.models_.ETLTask import ETLStatus, ETLType
from plenario.views import approve_dataset
from tests.test_fixtures.post_data import roadworks_post_data, roadworks_post_data_admin


def cleanup():
    try:
        session.execute('delete from etl_task')
        session.execute('drop table roadworks')
        session.commit()
    except:
        session.rollback()


def wait_for(dataset, status, seconds):
    for i in range(0, seconds * 2):
        time.sleep(0.5)
        task = fetch_task(dataset)
        if task.status == status:
            break


class TestETLTask(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        cls.app = create_app()
        cls.test = cls.app.test_client()
        cleanup()

    def test_01_add_task(self):

        add_task('foo_dset_name', 'foo_type')

        task = session.execute(
            "select * from etl_task where dataset_name = 'foo_dset_name'"
        ).first()

        self.assertEqual(task.dataset_name, 'foo_dset_name')
        self.assertEqual(task.status, ETLStatus['pending'])
        self.assertEqual(task.error, None)
        self.assertEqual(task.type, 'foo_type')

    def test_02_update_task(self):

        add_task('foo_dset_name', 'foo_type')
        update_task('foo_dset_name', '2000-01-01', 'new_status', 'terrible_error')

        task = session.execute(
            "select * from etl_task where dataset_name = 'foo_dset_name'"
        ).first()

        self.assertEqual(task.dataset_name, 'foo_dset_name')
        self.assertEqual(task.status, 'new_status')
        self.assertEqual(task.error, 'terrible_error')
        self.assertEqual(task.type, 'foo_type')

    def test_03_fetch_task(self):

        add_task('foo_dset_name', 'foo_type')

        expected_result = session.execute(
            "select * from etl_task where dataset_name = 'foo_dset_name'"
        ).first()
        actual_result = fetch_task('foo_dset_name')

        self.assertEqual(expected_result.dataset_name, actual_result.dataset_name)
        self.assertEqual(expected_result.status, actual_result.status)
        self.assertEqual(expected_result.error, actual_result.error)
        self.assertEqual(expected_result.type, actual_result.type)

    def test_04_fetch_etl_status(self):

        add_task('roadworks', ETLType['dataset'])
        add_task('flu_shot_clinics', ETLType['dataset'])
        add_task('neighborhoods', ETLType['shapeset'])

        tasks = fetch_table_etl_status('master')
        for task in tasks:
            print task

        self.assertGreaterEqual(len(tasks), 2)

        for task in tasks:
            self.assertEqual(task.type, 'master')

        self.assertEqual(tasks[0], 'Ingest Pending')

    def test_05_task_submitted_with_approve_dataset(self):
        # Simulate a user submitting a request for roadworks.
        try:
            self.test.post('/add', data=roadworks_post_data)
        except IntegrityError:
            # It's okay, the record for this has already been submitted.
            pass

        # Get the source_url_hash.
        url_hash = session.execute(
            "select source_url_hash "
            "from meta_master where dataset_name = 'roadworks'"
        ).scalar()

        # Approve the dataset as an admin.
        approve_dataset(url_hash)

        # Ensure that a task was added.
        task = fetch_task('roadworks')
        self.assertEqual(task.dataset_name, 'roadworks')
        self.assertEqual(task.status, ETLStatus['pending'])
        self.assertEqual(task.error, None)
        self.assertEqual(task.type, ETLType['dataset'])

    # def test_task_submitted_with_admin_add_dataset(self):
    #
    #     session.execute('delete from etl_task')
    #     session.commit()
    #
    #     # Simulate an admin submitting a dataset.
    #     try:
    #         response = self.test.post('/admin/add', data=roadworks_post_data, follow_redirects=False)
    #     except IntegrityError:
    #         pass
    #
    #     # An admin add should just create a task straight away without
    #     # requiring approval.
    #     task = fetch_task('roadworks')
    #     self.assertEqual(task.dataset_name, 'roadworks')
    #     self.assertEqual(task.status, ETLStatus['pending'])
    #     self.assertEqual(task.error, None)
    #     self.assertEqual(task.type, ETLType['dataset'])
    #
    #     # Clean up for the other tests.
    #     session.execute('delete from etl_task')
    #     session.commit()

    def test_07_task_updated_during_etl(self):

        # Give the worker some time to grab the job and update the status.
        wait_for('roadworks', ETLStatus['started'], 7)

        task = fetch_task('roadworks')
        self.assertEqual(task.dataset_name, 'roadworks')
        self.assertEqual(task.status, ETLStatus['started'])
        self.assertEqual(task.error, None)
        self.assertEqual(task.type, ETLType['dataset'])

    def test_08_task_updated_after_etl_completes(self):

        # Give the worker some time to grab the job and update the status.
        wait_for('roadworks', ETLStatus['success'], 7)

        task = fetch_task('roadworks')
        self.assertEqual(task.dataset_name, 'roadworks')
        self.assertEqual(task.status, ETLStatus['success'])
        self.assertEqual(task.error, None)
        self.assertEqual(task.type, ETLType['dataset'])

    def test_09_remove_task(self):

        delete_task('foo_dset_name')
        delete_task('roadworks')

        task = session.execute(
            "select * from etl_task where dataset_name = 'foo_dset_name'"
        ).first()

        self.assertIsNone(task)

        task = session.execute(
            "select * from etl_task where dataset_name = 'roadworks'"
        ).first()

        self.assertFalse(task)

    @classmethod
    def tearDownClass(cls):

        cleanup()
