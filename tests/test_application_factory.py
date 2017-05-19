from unittest import TestCase
from plenario import create_app
from os import environ


class TestApplicationFactory(TestCase):

   def test_application_created_with_dev_config(self):
      app = create_app('plenario.settings.DevConfig')
      self.assertEqual(app.config['DB_NAME'], 'plenario_dev')

   def test_application_created_with_test_config(self):
      app = create_app('plenario.settings.TestConfig')
      self.assertEqual(app.config['DB_NAME'], 'plenario_test')
