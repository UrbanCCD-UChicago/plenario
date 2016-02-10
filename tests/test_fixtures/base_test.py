import os

import unittest

from tests.test_fixtures.point_meta import flu_shot_meta, landmarks_meta, \
    flu_path, landmarks_path, crime_meta, crime_path
from plenario.models import MetaTable
from plenario.etl.point import PlenarioETL

from init_db import init_meta
from plenario import create_app
from plenario.database import session
from plenario.etl.shape import ShapeETL
from plenario.models import ShapeMetadata

pwd = os.path.dirname(os.path.realpath(__file__))

fixtures_path = pwd
FIXTURE_PATH = pwd

def ingest_from_fixture(fixture_meta, fname):
    md = MetaTable(**fixture_meta)
    session.add(md)
    session.commit()
    path = os.path.join(fixtures_path, fname)
    point_etl = PlenarioETL(md, source_path=path)
    point_etl.add()


def drop_tables(table_names):
    drop_template = 'DROP TABLE IF EXISTS {};'
    command = ''.join([drop_template.format(table_name) for table_name in table_names])
    session.execute(command)
    session.commit()


class Fixture(object):
    def __init__(self, human_name, file_name):
        self.human_name = human_name
        self.table_name = ShapeMetadata.make_table_name(human_name)
        self.path = os.path.join(FIXTURE_PATH, file_name)
        self.update_freq = 'yearly'

fixtures = {
    'city': Fixture(human_name=u'Chicago City Limits',
                    file_name='chicago_city_limits.zip'),
    'streets': Fixture(human_name=u'Pedestrian Streets',
                       file_name='chicago_pedestrian_streets.zip'),
    'zips': Fixture(human_name=u'Zip Codes',
                    file_name='chicago_zip_codes.zip'),
    'neighborhoods': Fixture(human_name=u'Chicago Neighborhoods',
                             file_name='chicago_neighborhoods.zip'),
    'changed_neighborhoods': Fixture(human_name=u'Chicago Neighborhoods',
        file_name='chicago_neighborhoods_changed.zip',)
}


class BasePlenarioTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls, shutdown=False):

        # Remove tables that we're about to recreate.
        # This doesn't happen in teardown because I find it helpful
        # to inspect them in the DB after running the tests.
        meta_table_names = ['meta_shape']
        fixture_table_names = [fixture.table_name for key, fixture in fixtures.iteritems()]

        drop_tables(meta_table_names + fixture_table_names)

        # Re-add meta tables
        init_meta()

        # Fully ingest the fixtures
        BasePlenarioTest.ingest_fixture(fixtures['city'])
        BasePlenarioTest.ingest_fixture(fixtures['streets'])
        BasePlenarioTest.ingest_fixture(fixtures['zips'])
        BasePlenarioTest.ingest_fixture(fixtures['neighborhoods'])

        # Add a dummy dataset to the metadata without ingesting a shapefile for it
        cls.dummy_name = ShapeMetadata.add(human_name=u'Dummy Name',
                                           source_url=None,
                                           update_freq='yearly',
                                           approved_status=False).dataset_name
        session.commit()

        tables_to_drop = [
            'flu_shot_clinics',
            'landmarks',
            'crimes',
            'meta_master'
        ]
        drop_tables(tables_to_drop)

        init_meta()

        ingest_from_fixture(flu_shot_meta, flu_path)
        ingest_from_fixture(landmarks_meta, landmarks_path)
        ingest_from_fixture(crime_meta, crime_path)

        cls.app = create_app().test_client()

        '''/detail'''

    @staticmethod
    def ingest_fixture(fixture):
        # Add the fixture to the metadata first
        shape_meta = ShapeMetadata.add(human_name=fixture.human_name,
                                       source_url=None,
                                       update_freq=fixture.update_freq,
                                       approved_status=False)
        session.commit()
        # Bypass the celery task and call on a ShapeETL directly
        ShapeETL(meta=shape_meta, source_path=fixture.path).add()
        return shape_meta

