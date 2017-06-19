import os
import unittest

from tests.fixtures.point_meta import flu_shot_meta, landmarks_meta
from tests.fixtures.point_meta import flu_path, landmarks_path
from tests.fixtures.point_meta import crime_meta, crime_path

from plenario import create_app
from plenario.database import postgres_session
from plenario.etl.point import PlenarioETL
from plenario.etl.shape import ShapeETL
from plenario.models import MetaTable, ShapeMetadata

from manage import init

pwd = os.path.dirname(os.path.realpath(__file__))

fixtures_path = pwd
FIXTURE_PATH = pwd


def ingest_point_fixture(fixture_meta, fname):
    md = MetaTable(**fixture_meta)
    postgres_session.add(md)
    postgres_session.commit()
    path = os.path.join(fixtures_path, fname)
    point_etl = PlenarioETL(md, source_path=path)
    point_etl.add()


def drop_tables(table_names):
    drop_template = 'DROP TABLE IF EXISTS {};'
    command = ''.join([drop_template.format(table_name) for table_name in table_names])
    postgres_session.execute(command)
    postgres_session.commit()


class ShapeFixture(object):
    def __init__(self, human_name, file_name):
        self.human_name = human_name
        self.table_name = ShapeMetadata.make_table_name(human_name)
        self.path = os.path.join(FIXTURE_PATH, file_name)
        self.update_freq = 'yearly'


shape_fixtures = {
    'city': ShapeFixture(human_name='Chicago City Limits',
                         file_name='chicago_city_limits.zip'),
    'streets': ShapeFixture(human_name='Pedestrian Streets',
                            file_name='chicago_pedestrian_streets.zip'),
    'zips': ShapeFixture(human_name='Zip Codes',
                         file_name='chicago_zip_codes.zip'),
    'neighborhoods': ShapeFixture(human_name='Chicago Neighborhoods',
                                  file_name='chicago_neighborhoods.zip'),
    'changed_neighborhoods': ShapeFixture(human_name='Chicago Neighborhoods',
                                          file_name='chicago_neighborhoods_changed.zip', )
}


class BasePlenarioTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls, shutdown=False):
        # Remove tables that we're about to recreate.
        # This doesn't happen in teardown because I find it helpful
        # to inspect them in the DB after running the tests.

        meta_table_names = ['meta_master', 'meta_shape', 'etl_task']
        drop_tables(meta_table_names)

        # Re-add meta tables
        init()

        cls.app = create_app().test_client()

    @classmethod
    def ingest_shapes(cls):
        fixtures = [f for k, f in shape_fixtures.items() if k != 'changed_neighborhoods']
        fixture_table_names = [f.table_name for f in fixtures]
        drop_tables(fixture_table_names)
        postgres_session.commit()

        for fixture in fixtures:
            cls.ingest_fixture(fixture)

        # Add a dummy dataset to the metadata without ingesting a shapefile for it
        cls.dummy_name = ShapeMetadata.add(human_name='Dummy Name',
                                           source_url=None,
                                           update_freq='yearly',
                                           approved_status=False).dataset_name
        postgres_session.commit()

    @classmethod
    def ingest_points(cls):
        drop_tables(("flu_shot_clinics", "landmarks", "crimes"))
        ingest_point_fixture(flu_shot_meta, flu_path)
        ingest_point_fixture(landmarks_meta, landmarks_path)
        ingest_point_fixture(crime_meta, crime_path)
        postgres_session.commit()

    @staticmethod
    def ingest_fixture(fixture):
        # Add the fixture to the metadata first
        shape_meta = ShapeMetadata.add(human_name=fixture.human_name,
                                       source_url=None,
                                       update_freq=fixture.update_freq,
                                       approved_status=False)
        postgres_session.commit()
        # Bypass the celery task and call on a ShapeETL directly
        ShapeETL(meta=shape_meta, source_path=fixture.path).add()
        return shape_meta

    @classmethod
    def tearDownClass(cls):
        postgres_session.close()
