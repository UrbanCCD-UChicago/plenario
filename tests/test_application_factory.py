from unittest import TestCase
from plenario import create_app


class TestApplicationFactory(TestCase):

   def test_application_created_with_default_config(self):
      app = create_app()
      self.assertEqual(app.config['DB_NAME'], 'plenario_test')

   def test_application_created_with_test_config(self):
      app = create_app('plenario.settings.TestConfig')
      self.assertEqual(app.config['DB_NAME'], 'test_plenario')
