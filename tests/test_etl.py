import os
from datetime import datetime
from os.path import join
from unittest import TestCase

from sqlalchemy import select, MetaData
from sqlalchemy.exc import ProgrammingError

from plenario.database import create_extension, drop_extension
from plenario.database import postgres_session, postgres_engine
from plenario.etl import ingest_points, ingest_shapes
from plenario.models import MetaTable, ShapeMetadata
from plenario.models.meta.schema import infer_local

pwd = os.path.dirname(os.path.realpath(__file__))
fixtures_path = os.path.join(pwd, './fixtures')


class BaseEventTest(TestCase):

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


class TestColumnInference(BaseEventTest):

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


class TestPointIngest(BaseEventTest):

    def test_insert_events(self):
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


class BaseShapeTest(TestCase):

    def setUp(self):
        for extension in {'plv8', 'postgis'}:
            try:
                create_extension(postgres_engine, extension)
            except ProgrammingError:
                pass

        ShapeMetadata.__table__.create(bind=postgres_engine, checkfirst=True)

    def tearDown(self):
        postgres_session.close()

        for extension in {'plv8', 'postgis'}:
            drop_extension(postgres_engine, extension)

        metadata = MetaData()
        metadata.reflect(bind=postgres_engine)
        metadata.drop_all(bind=postgres_engine)


class TestShapeIngest(BaseShapeTest):

    def test_insert_shapes(self):
        city_limits = ShapeMetadata(
            dataset_name='chicago_neighborhoods',
            source_url=join(fixtures_path, 'chicago_neighborhoods_changed.zip')
        )

        table = ingest_shapes(city_limits, local=True)
        sel = table.select().where(table.c['sec_neigh'] == 'ENGLEWOOD')
        res = postgres_engine.execute(sel).fetchall()
        altered_value = res[0]['pri_neigh']

        self.assertEqual(altered_value, 'Englerwood')

    def test_no_import_when_name_conflict(self):
        city_limits = ShapeMetadata(
            dataset_name='chicago_neighborhoods',
            source_url=join(fixtures_path, 'chicago_neighborhoods_changed.zip')
        )

        ingest_shapes(city_limits, local=True)
        with self.assertRaises(Exception):
            ingest_shapes(city_limits, local=True)
            postgres_session.rollback()

    def test_delete_shape(self):
        # Can we remove a shape that's fully ingested?
        city_meta = postgres_session.query(ShapeMetadata).get(shape_fixtures['city'].table_name)
        self.assertIsNotNone(city_meta)
        city_meta.remove_table()
        postgres_session.commit()
        city_meta = postgres_session.query(ShapeMetadata).get(shape_fixtures['city'].table_name)
        self.assertIsNone(city_meta)

        # Can we remove a shape that's only in the metadata?
        dummy_meta = postgres_session.query(ShapeMetadata).get(self.dummy_name)
        self.assertIsNotNone(dummy_meta)
        dummy_meta.remove_table()
        postgres_session.commit()
        dummy_meta = postgres_session.query(ShapeMetadata).get(self.dummy_name)
        self.assertIsNone(dummy_meta)

        # Add them back to return to original test state
        ShapeTests.ingest_fixture(shape_fixtures['city'])
        ShapeMetadata.add(human_name='Dummy Name',
                          source_url=None,
                          update_freq='yearly',
                          approved_status=False)

        postgres_session.commit()
