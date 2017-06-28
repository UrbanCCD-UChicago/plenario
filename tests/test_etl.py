import os
from datetime import datetime
from os.path import join
from unittest import TestCase

from sqlalchemy import select, MetaData
from sqlalchemy.exc import ProgrammingError

from plenario.database import create_extension, drop_extension
from plenario.database import postgres_session, postgres_engine
from plenario.etl import ingest_points
from plenario.models import MetaTable
from plenario.models.meta.schema import infer_local

pwd = os.path.dirname(os.path.realpath(__file__))
fixtures_path = os.path.join(pwd, './fixtures')


class BaseTest(TestCase):

    def setUp(self):
        for extension in {'plv8', 'postgis'}:
            try:
                create_extension(postgres_engine, extension)
            except ProgrammingError:
                pass

        MetaTable.__table__.create(bind=postgres_engine, checkfirst=True)

    def tearDown(self):
        postgres_session.close()

        for extension in {'plv8', 'postgis'}:
            drop_extension(postgres_engine, extension)

        metadata = MetaData()
        metadata.reflect(bind=postgres_engine)
        metadata.drop_all(bind=postgres_engine)


class TestColumnInference(BaseTest):

    def test_column_inference(self):
        radio_path = os.path.join(fixtures_path, 'community_radio_events.csv')

        with open(radio_path, 'rb') as radios:
            columns = infer_local(radios)

        expected_types = {
            'event_name': 'VARCHAR',
            'date': 'DATETIME',
            'lat': 'FLOAT',
            'lon': 'FLOAT'
        }

        for column in columns:
            name = column.name
            dtype = column.type
            self.assertEqual(expected_types[name], str(dtype))


class TestPointIngest(BaseTest):

    def test_insert_data(self):
        permits = MetaTable(
            url=join(fixtures_path, 'dog_park_permits.csv'),
            human_name='Dog Park Permits',
            observed_date='Date',
            latitude='lat',
            longitude='lon',
            approved_status=True
        )

        table = ingest_points(permits, local=True)
        rows = postgres_engine.execute(table.select()).fetchall()
        self.assertEqual(len(rows), 5)

    def test_update_no_change(self):
        permits = MetaTable(
            url=join(fixtures_path, 'dog_park_permits.csv'),
            human_name='Dog Park Permits',
            observed_date='Date',
            latitude='lat',
            longitude='lon',
            approved_status=True
        )

        table = ingest_points(permits, local=True)
        rows = postgres_engine.execute(table.select()).fetchall()
        self.assertEqual(len(rows), 5)

    def test_update_with_delete(self):
        permits_minus_one = MetaTable(
            url=join(fixtures_path, 'dog_park_permits_deleted.csv'),
            human_name='Dog Park Permits',
            observed_date='Date',
            latitude='lat',
            longitude='lon',
            approved_status=True
        )

        table = ingest_points(permits_minus_one, local=True)
        rows = postgres_engine.execute(table.select()).fetchall()
        self.assertEqual(len(rows), 4)

    def test_update_with_change(self):
        radios_changed = MetaTable(
            url=join(fixtures_path, 'community_radio_events_changed.csv'),
            human_name='Community Radio Events',
            observed_date='Date',
            latitude='lat',
            longitude='lon',
            approved_status=True
        )

        table = ingest_points(radios_changed, local=True)
        rows = postgres_engine.execute(table.select()).fetchall()
        self.assertEqual(len(rows), 5)

        sel = select([table.c.date]).where(table.c.event_name == 'baz')
        changed_date = postgres_engine.execute(sel).fetchone()[0]
        self.assertEqual(changed_date, datetime(1993, 11, 10, 0, 0))

    def test_ingested_table_has_bbox(self):
        postgres_engine.execute(MetaTable.__table__.delete())

        radios = MetaTable(
            url=join(fixtures_path, 'community_radio_events.csv'),
            human_name='Community Radio Events',
            observed_date='Date',
            latitude='lat',
            longitude='lon',
            approved_status=True
        )

        postgres_session.add(radios)
        postgres_session.commit()

        table = ingest_points(radios, local=True)
        rows = postgres_engine.execute(table.select()).fetchall()
        bbox = MetaTable.get_by_dataset_name('community_radio_events').bbox

        self.assertEqual(len(rows), 5)
        self.assertIsNotNone(bbox)

    def test_new_table_has_correct_column_names_in_meta(self):
        postgres_engine.execute(MetaTable.__table__.delete())

        radios = MetaTable(
            url=join(fixtures_path, 'community_radio_events.csv'),
            human_name='Community Radio Events',
            observed_date='Date',
            latitude='lat',
            longitude='lon',
            approved_status=True
        )

        ingest_points(radios, local=True)

        columns = postgres_session.query(MetaTable.column_names)
        columns = columns.filter(MetaTable.dataset_name == 'community_radio_events')
        columns = columns.first()[0]

        self.assertEqual(len(columns), 4)
