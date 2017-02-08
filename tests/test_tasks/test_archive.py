import unittest

from datetime import datetime, timedelta
from sqlalchemy import MetaData
from sqlalchemy import Table, Column, String, DateTime, Float
from sqlalchemy.orm import sessionmaker

from plenario.database import app_engine as engine
from plenario.tasks import archive


metadata = MetaData()

temperature = Table(
    'temperature',
    metadata,
    Column('node_id', String),
    Column('datetime', DateTime),
    Column('meta_id', Float),
    Column('sensor', String))


class TestArchive(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        cls.session = sessionmaker(bind=engine)()
        temperature.create(engine)

        time = datetime.now()
        time = time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        for i in range(0, 100):
            insert = temperature.insert().values(
                node_id='0',
                datetime=time + timedelta(days=i),
                meta_id='0',
                sensor='0')
            cls.session.execute(insert)
        cls.session.commit()

    def test_archive(self):

        archive()

    @classmethod
    def tearDownClass(cls):

        temperature.drop()
        cls.session.close()
