from os import environ
from sqlalchemy import create_engine
from unittest import TestCase

from plenario import create_app
from plenario.database import create_database, create_extension, drop_database
from plenario.database import postgres_session, postgres_engine
from plenario.models import ShapeMetadata
from plenario.settings import Config, DATABASE_CONN
from plenario.tasks import add_shape


class TestShapeETL(TestCase):

    def setUp(self):
        self.base_engine = create_engine(DATABASE_CONN)
        create_database(self.base_engine, Config.DB_NAME)
        self.engine = create_engine(Config.DATABASE_CONN)
        create_extension(self.engine, 'postgis')
        ShapeMetadata.__table__.create(bind=self.engine)
        self.client = create_app('plenario.settings.Config').test_client()

    def tearDown(self):
        self.engine.dispose()
        postgres_session.close()
        postgres_engine.dispose()
        drop_database(self.base_engine, Config.DB_NAME)

    def post_senior_centers(self):
        post_url = '/add?is_shapefile=true&dataset_url='
        post_url += 'https://www.dropbox.com/sh/mctkz58l4x45u25/AAA6V1ke56QefkgwCykq3kHga?dl=1'
        return self.client.post('/add?is_shapefile=true', data={
            'dataset_name': 'senior_centers',
            'file_url':  'https://www.dropbox.com/sh/mctkz58l4x45u25/AAA6V1ke56QefkgwCykq3kHga?dl=1',
            'update_frequency': 'daily',
            'contributor_name': 'jesse',
            'contributor_email': 'jbracho@uchicago.edu'
        })

    def test_shape_meta_submission(self):
        response = self.post_senior_centers()
        self.assertEqual(response.status_code, 302)
        senior_centers = ShapeMetadata.query.first()
        self.assertTrue(senior_centers)
        self.assertEqual(senior_centers.dataset_name, 'senior_centers')
        self.assertEqual(senior_centers.source_url, 'https://www.dropbox.com/sh/mctkz58l4x45u25/AAA6V1ke56QefkgwCykq3kHga?dl=1')
        self.assertEqual(senior_centers.update_freq, 'daily')
        self.assertEqual(senior_centers.contributor_name, 'jesse')
        self.assertEqual(senior_centers.contributor_email, 'jbracho@uchicago.edu')
        self.assertFalse(senior_centers.approved_status)

    def test_shape_meta_ingest(self):
        response = self.post_senior_centers()
        self.assertEqual(response.status_code, 302)
        senior_centers = add_shape('senior_centers')
        senior_centers_count = postgres_session.query(senior_centers).count()
        self.assertEqual(senior_centers_count, 21)
