import unittest
from plenario import create_app
from plenario.utils.model_helpers import *
from tests.test_fixtures.post_data import *


class TestModelHelpers(unittest.TestCase):

    def setUp(self):
        self.app = create_app()
        self.test_app = self.app.test_client()

    def test_meta_exists(self):
        add_meta_if_not_exists(app=self.test_app,
                               table_name='restaurant_applications',
                               post_data=restaurants_post_data,
                               shape=False)
        self.assertTrue(meta_exists('restaurant_applications', shape=False))

    def test_meta_exists_bad_arg(self):
        self.assertFalse(meta_exists('foo', shape=False))

    def test_meta_exists_shape(self):
        add_meta_if_not_exists(app=self.test_app,
                               table_name='boundaries_neighborhoods',
                               post_data=boundaries_post_data,
                               shape=True)
        self.assertTrue(meta_exists('boundaries_neighborhoods', shape=True))

    def test_meta_exists_shape_bad_arg(self):
        add_meta_if_not_exists(app=self.test_app,
                               table_name='boundaries_neighborhoods',
                               post_data=boundaries_post_data,
                               shape=True)
        self.assertFalse(meta_exists('foo', shape=True))

    def test_table_exists(self):
        add_meta_if_not_exists(app=self.test_app,
                               table_name='restaurant_applications',
                               post_data=restaurants_post_data,
                               shape=False)
        add_table_if_not_exists('restaurant_applications')
        arg = 'restaurant_applications'
        result = table_exists(arg)
        self.assertTrue(result)

    def test_table_exists_bad_arg(self):
        self.assertFalse(table_exists('foo'))

    def test_table_exists_shape(self):
        add_meta_if_not_exists(app=self.test_app,
                               table_name='boundaries_neighborhoods',
                               post_data=boundaries_post_data,
                               shape=True)
        add_table_if_not_exists('boundaries_neighborhoods', shape=True)
        self.assertTrue(table_exists('boundaries_neighborhoods'))

    def test_drop_table_if_exists(self):
        add_meta_if_not_exists(app=self.test_app,
                               table_name='restaurant_applications',
                               post_data=restaurants_post_data,
                               shape=False)
        add_meta_if_not_exists(app=self.test_app,
                               table_name='boundaries_neighborhoods',
                               post_data=boundaries_post_data,
                               shape=True)
        drop_table_if_exists('restaurant_applications', shape=False)
        drop_table_if_exists('boundaries_neighborhoods', shape=True)

        self.assertFalse(table_exists('restaurant_applications'))
        self.assertFalse(table_exists('boundaries_neighborhoods'))

    def test_drop_meta_if_exists(self):
        drop_meta_if_exists('restaurant_applications', shape=False)
        drop_meta_if_exists('boundaries_neighborhoods', shape=True)

        self.assertFalse(meta_exists('restaurant_applications', shape=False))
        self.assertFalse(meta_exists('boundaries_neighborhoods', shape=True))

    def test_meta_exists_but_not_table(self):
        add_meta_if_not_exists(app=self.test_app,
                               table_name='restaurant_applications',
                               post_data=restaurants_post_data,
                               shape=False)
        add_meta_if_not_exists(app=self.test_app,
                               table_name='boundaries_neighborhoods',
                               post_data=boundaries_post_data,
                               shape=True)
        drop_table_if_exists('restaurant_applications', shape=False)
        drop_table_if_exists('boundaries_neighborhoods', shape=True)

        self.assertTrue(meta_exists('restaurant_applications', shape=False))
        self.assertTrue(meta_exists('boundaries_neighborhoods', shape=True))
        self.assertFalse(table_exists('restaurant_applications'))
        self.assertFalse(table_exists('boundaries_neighborhoods'))

    def test_fetch_meta(self):
        add_meta_if_not_exists(app=self.test_app,
                               table_name='restaurant_applications',
                               post_data=restaurants_post_data,
                               shape=False)
        meta = fetch_meta('restaurant_applications')
        self.assertEqual(meta.dataset_name, 'restaurant_applications')

    def test_fetch_table(self):
        add_meta_if_not_exists(app=self.test_app,
                               table_name='boundaries_neighborhoods',
                               post_data=boundaries_post_data,
                               shape=True)
        add_table_if_not_exists('boundaries_neighborhoods', shape=True)
        table = fetch_table('boundaries_neighborhoods', shape=True)

        self.assertEqual(table.name, 'boundaries_neighborhoods')

    # Test: fetch_pending_tables -- difficult to do right now because
    # of interference from the base test suite.
