import json
import os
from datetime import date
from unittest import TestCase

import sqlalchemy as sa
from geoalchemy2 import Geometry
from sqlalchemy import Table, Column, Integer, Date, Float, String, TIMESTAMP, MetaData
from sqlalchemy.exc import NoSuchTableError

from manage import init
from plenario.database import postgres_session, postgres_engine
from plenario.etl.point import Staging, PlenarioETL
from plenario.models import MetaTable

pwd = os.path.dirname(os.path.realpath(__file__))
fixtures_path = os.path.join(pwd, '../fixtures')


def drop_if_exists(table_name):
    try:
        t = Table(table_name, MetaData(), extend_existing=True)
        t.drop(postgres_engine, checkfirst=True)
    except NoSuchTableError:
        pass


def drop_meta(table_name):
    del_ = "DELETE FROM meta_master WHERE dataset_name = '{}';".format(table_name)
    postgres_engine.execute(del_)


class StagingTableTests(TestCase):
    """
    Given a dataset is present in MetaTable,
    can we grab a current csv of the underlying data from wherever it lives
    and then make that into a free-standing table?
    """

    @classmethod
    def setUpClass(cls):
        init()

        cls.dog_path = os.path.join(fixtures_path, 'dog_park_permits.csv')
        cls.radio_path = os.path.join(fixtures_path, 'community_radio_events.csv')
        cls.opera_path = os.path.join(fixtures_path, 'public_opera_performances.csv')

        cls.expected_radio_col_names = ['lat', 'lon', 'event_name', 'date']
        cls.expected_dog_col_names = ['lat', 'lon', 'hooded_figure_id', 'date']

    def setUp(self):
        postgres_session.rollback()
        # Ensure we have metadata loaded into the database
        # to mimic the behavior of metadata ingestion preceding file ingestion.
        drop_meta('dog_park_permits')
        drop_meta('community_radio_events')
        drop_meta('public_opera_performances')

        # Make new MetaTable objects
        self.unloaded_meta = MetaTable(url='nightvale.gov/events.csv',
                                       human_name='Community Radio Events',
                                       business_key='Event Name',
                                       observed_date='Date',
                                       latitude='lat', longitude='lon',
                                       approved_status=True)

        self.existing_meta = MetaTable(url='nightvale.gov/dogpark.csv',
                                       human_name='Dog Park Permits',
                                       business_key='Hooded Figure ID',
                                       observed_date='Date',
                                       latitude='lat', longitude='lon',
                                       approved_status=False)

        self.opera_meta = MetaTable(url='nightvale.gov/opera.csv',
                                    human_name='Public Opera Performances',
                                    business_key='Event Name',
                                    observed_date='Date',
                                    location='Location',
                                    approved_status=False)
        postgres_session.add_all([self.existing_meta, self.opera_meta, self.unloaded_meta])
        postgres_session.commit()

        # Also, let's have one table pre-loaded...
        self.existing_table = sa.Table('dog_park_permits', MetaData(),
                                       Column('hooded_figure_id', Integer),
                                       Column('point_date', TIMESTAMP, nullable=False),
                                       Column('date', Date, nullable=True),
                                       Column('lat', Float, nullable=False),
                                       Column('lon', Float, nullable=False),
                                       Column('hash', String(32), primary_key=True),
                                       Column('geom', Geometry('POINT', srid=4326), nullable=True))
        drop_if_exists(self.existing_table.name)
        self.existing_table.create(bind=postgres_engine)

        # ... with some pre-existing data
        ins = self.existing_table.insert().values(hooded_figure_id=1,
                                                  point_date=date(2015, 1, 2),
                                                  lon=-87.6495076896,
                                                  lat=41.7915865543,
                                                  geom=None,
                                                  hash='addde9be7f59e95fc08e54e29b2a947f')
        postgres_engine.execute(ins)

    def tearDown(self):
        postgres_session.close()

    '''
    Do the names of created columns match what we expect?
    Would be nice to check types too, but that was too fragile.
    '''

    @staticmethod
    def extract_names(columns):
        return [c.name for c in columns]

    def test_col_info_infer(self):
        with Staging(self.unloaded_meta, source_path=self.radio_path)as s_table:
            observed_names = self.extract_names(s_table.cols)
        self.assertEqual(set(observed_names), set(self.expected_radio_col_names))

    def test_col_info_existing(self):
        with Staging(self.existing_meta, source_path=self.dog_path) as s_table:
            observed_col_names = self.extract_names(s_table.cols)
        self.assertEqual(set(observed_col_names), set(self.expected_dog_col_names))

    def test_col_info_provided(self):
        # The frontend should send back strings compatible with the COL_VALUES in etl.point
        col_info_raw = [('event_name', 'string'),
                        ('date', 'date'),
                        ('lat', 'float'),
                        ('lon', 'float')]
        stored_col_info = [{'field_name': name, 'data_type': d_type}
                           for name, d_type in col_info_raw]
        self.unloaded_meta.contributed_data_types = json.dumps(stored_col_info)
        with Staging(self.unloaded_meta, source_path=self.radio_path) as s_table:
            observed_names = self.extract_names(s_table.cols)
            self.assertEqual(set(observed_names), set(self.expected_radio_col_names))

    '''
    Are the files ingested as we expect?
    '''

    def test_staging_new_table(self):
        # For the entry in MetaTable without a table, create a staging table.
        # We'll need to read from a fixture csv.
        with Staging(self.unloaded_meta, source_path=self.radio_path) as s_table:
            with postgres_engine.begin() as connection:
                all_rows = connection.execute(s_table.table.select()).fetchall()
        self.assertEqual(len(all_rows), 5)

    def test_staging_existing_table(self):
        # With a fixture CSV whose columns match the existing dataset,
        # create a staging table.
        with Staging(self.existing_meta, source_path=self.dog_path) as s_table:
            with postgres_engine.begin() as connection:
                all_rows = connection.execute(s_table.table.select()).fetchall()
        self.assertEqual(len(all_rows), 5)

    def test_insert_data(self):
        etl = PlenarioETL(self.existing_meta, source_path=self.dog_path)
        etl.update()

        existing = self.existing_table
        all_rows = postgres_session.execute(existing.select()).fetchall()
        self.assertEqual(len(all_rows), 5)

    def test_update_no_change(self):
        etl = PlenarioETL(self.existing_meta, source_path=self.dog_path)
        etl.update()

        # We're just checking that an exception doesn't get thrown.
        etl = PlenarioETL(self.existing_meta, source_path=self.dog_path)
        etl.update()

    def test_update_with_delete(self):
        etl = PlenarioETL(self.existing_meta, source_path=self.dog_path)
        etl.update()

        # The same source CSV, but with one less record
        deleted_path = os.path.join(fixtures_path, 'dog_park_permits_deleted.csv')
        etl = PlenarioETL(self.existing_meta, source_path=deleted_path)
        etl.update()

        all_rows = postgres_session.execute(self.existing_table.select()).fetchall()
        self.assertEqual(len(all_rows), 4)

    def test_update_with_change(self):
        drop_if_exists(self.unloaded_meta.dataset_name)

        etl = PlenarioETL(self.unloaded_meta, source_path=self.radio_path)
        table = etl.add()

        changed_path = os.path.join(fixtures_path, 'community_radio_events_changed.csv')
        etl = PlenarioETL(self.unloaded_meta, source_path=changed_path)
        etl.update()

        sel = sa.select([table.c.date]).where(table.c.event_name == 'baz')
        changed_date = postgres_engine.execute(sel).fetchone()[0]
        self.assertEqual(changed_date, date(1993, 11, 10))

    def test_new_table(self):
        drop_if_exists(self.unloaded_meta.dataset_name)

        etl = PlenarioETL(self.unloaded_meta, source_path=self.radio_path)
        new_table = etl.add()

        all_rows = postgres_session.execute(new_table.select()).fetchall()
        self.assertEqual(len(all_rows), 5)
        postgres_session.close()
        new_table.drop(postgres_engine, checkfirst=True)

        # Did we add a bbox?
        bbox = MetaTable.get_by_dataset_name('community_radio_events').bbox
        self.assertIsNotNone(bbox)

    def test_new_table_has_correct_column_names_in_meta(self):
        drop_if_exists(self.unloaded_meta.dataset_name)

        etl = PlenarioETL(self.unloaded_meta, source_path=self.radio_path)
        new_table = etl.add()

        columns = postgres_session.query(MetaTable.column_names)
        columns = columns.filter(MetaTable.dataset_name == self.unloaded_meta.dataset_name)
        columns = columns.first()[0]

        self.assertEqual(len(columns), 4)

        postgres_session.close()
        new_table.drop(postgres_engine, checkfirst=True)

    def test_location_col_add(self):
        drop_if_exists(self.opera_meta.dataset_name)

        etl = PlenarioETL(self.opera_meta, source_path=self.opera_path)
        new_table = etl.add()

        all_rows = postgres_session.execute(new_table.select()).fetchall()
        self.assertEqual(len(all_rows), 5)
        postgres_session.close()
        new_table.drop(postgres_engine, checkfirst=True)

        # Did we add a bbox?
        bbox = MetaTable.get_by_dataset_name('public_opera_performances').bbox
        self.assertIsNotNone(bbox)

    def test_location_col_update(self):
        drop_if_exists(self.opera_meta.dataset_name)

        self.opera_table = sa.Table(self.opera_meta.dataset_name, MetaData(),
                                    Column('event_name', String, primary_key=True),
                                    Column('date', Date, nullable=True),
                                    Column('location', String, nullable=False),
                                    Column('geom', Geometry('POINT', srid=4326), nullable=True),
                                    Column('point_date', TIMESTAMP, nullable=False))
        drop_if_exists(self.existing_table.name)
        self.opera_table.create(bind=postgres_engine)

        ins = self.opera_table.insert().values(event_name='quux',
                                               date=None,
                                               point_date=date(2015, 1, 2),
                                               location='(-87.6495076896,41.7915865543)',
                                               geom=None)
        postgres_engine.execute(ins)
