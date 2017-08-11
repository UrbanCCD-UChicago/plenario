import os

from flask_testing import TestCase
from sqlalchemy.orm import sessionmaker

from plenario.database import postgres_engine
from plenario.etl.point import PlenarioETL
from plenario.models import MetaTable
from plenario.server import create_app

Session = sessionmaker()


class PlenarioBaseTest(TestCase):
    """This test class takes advantage of the sqlalchemy Connection object's
    ability to nest transactions. By wrapping a session in an enclosing
    transaction, the session can flush or commit and behave as though those
    changes were actually applied. After which, all of the changes can be
    rolled back, leaving the database as if nothing had happened."""

    def setUp(self):
        self.connection = postgres_engine.connect()
        self.transaction = self.connection.begin()
        self.session = Session(bind=self.connection)

    def tearDown(self):
        self.session.close()
        self.transaction.rollback()
        self.connection.close()

    def create_app(self):
        return create_app()


def load_fixture(fixture, name, url, date=None, location=None, latitude=None, longitude=None):
    """Helper for loading local point dataset fixtures. By providing different
    urls, you can load the same fixture as different datasets."""

    metatable = MetaTable(
        human_name=name,
        observed_date=date,
        location=location,
        latitude=latitude,
        longitude=longitude,
        url=url
    )

    path = os.path.join('tests/fixtures/', fixture)
    point_etl = PlenarioETL(metatable, source_path=path)
    point_etl.add()
