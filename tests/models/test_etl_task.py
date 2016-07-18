import unittest

from plenario import create_app
from plenario.database import session
from plenario.models_.ETLTask import add_task, fetch_task, delete_task
from plenario.models_.ETLTask import update_task, fetch_table_etl_status
from plenario.models_.ETLTask import ETLStatus, ETLType


class TestETLTask(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        cls.app = create_app()
        cls.test = cls.app.test_client()

    def test_add_task(self):

        add_task('foo_dset_name', 'foo_status', 'foo_error', 'foo_type')

        task = session.execute(
            "select * from etl_task where dataset_name = 'foo_dset_name'"
        ).first()

        self.assertEqual(task.dataset_name, 'foo_dset_name')
        self.assertEqual(task.status, 'foo_status')
        self.assertEqual(task.error, 'foo_error')
        self.assertEqual(task.type, 'foo_type')

    def test_remove_task(self):

        delete_task('foo_dset_name')

        task = session.execute(
            "select * from etl_task where dataset_name = 'foo_dset_name'"
        ).first()

        self.assertIsNone(task)

    def test_update_task(self):

        add_task('foo_dset_name', 'foo_status', 'foo_error', 'foo_type')
        update_task('foo_dset_name', 'new_status', 'terrible_error')

        task = session.execute(
            "select * from etl_task where dataset_name = 'foo_dset_name'"
        ).first()

        self.assertEqual(task.dataset_name, 'foo_dset_name')
        self.assertEqual(task.status, 'new_status')
        self.assertEqual(task.error, 'terrible_error')
        self.assertEqual(task.type, 'foo_type')

    def test_fetch_task(self):

        add_task('foo_dset_name', 'foo_status', 'foo_error', 'foo_type')

        expected_result = session.execute(
            "select * from etl_task where dataset_name = 'foo_dset_name'"
        ).first()
        actual_result = fetch_task('foo_dset_name')

        self.assertEqual(expected_result.dataset_name, actual_result.dataset_name)
        self.assertEqual(expected_result.status, actual_result.status)
        self.assertEqual(expected_result.error, actual_result.error)
        self.assertEqual(expected_result.type, actual_result.type)

    def test_fetch_etl_status(self):

        add_task('roadworks', ETLStatus['pending'], None, ETLType['dataset'])
        add_task('flu_shot_clinics', ETLStatus['success'], None, ETLType['dataset'])
        add_task('neighborhoods', ETLStatus['failure'], 'you goofed', ETLType['shapeset'])

        tasks = fetch_table_etl_status('master')

        self.assertEqual(len(tasks), 2)

        for task in tasks:
            self.assertEqual(task.type, 'master')

        road_task = tasks[0]
        flu_task = tasks[1]

        self.assertEqual(road_task.status, 'Ingest Pending')
        self.assertEqual(flu_task.status, 'Success')

    @classmethod
    def tearDownClass(cls):

        session.execute('delete from etl_task')
