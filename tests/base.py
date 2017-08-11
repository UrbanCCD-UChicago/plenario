from flask_testing import TestCase
from sqlalchemy.orm import sessionmaker

from plenario.database import postgres_engine


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
