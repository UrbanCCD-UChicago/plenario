import unittest

from sqlalchemy.exc import ProgrammingError, IntegrityError, InvalidRequestError

from plenario.database import redshift_engine
from plenario.database import postgres_session as postgres_session
from plenario.models.SensorNetwork import NetworkMeta, FeatureMeta


session = postgres_session()
objects = []


def redshift_table_exists(table_name):
    """Make an inexpensive query to the database. It the table does not exist,
    the query will cause a ProgrammingError.

    :param table_name: (string) table name
    :returns (bool) true if the table exists, false otherwise"""

    try:
        redshift_engine.execute("select '{}'::regclass".format(table_name))
        return True
    except ProgrammingError:
        return False


class TestFeatureMeta(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        network = NetworkMeta(name='test')
        feature = FeatureMeta(
            name='foo',
            networks=[network],
            observed_properties=[{
                'name': 'bar',
                'type': 'float'
            }, {
                'name': 'baz',
                'type': 'float'
            }]
        )

        objects.append(network)
        objects.append(feature)

        try:
            session.add(network)
            session.add(feature)
            session.commit()
        except IntegrityError:
            session.rollback()

    def test_feature_meta_mirror(self):
        """Redshift table is created for a call to mirror?"""

        feature = FeatureMeta.query.get('foo')
        feature.mirror()
        self.assert_(redshift_table_exists('test__foo'))

    @classmethod
    def tearDownClass(cls):

        for o in objects:
            try:
                session.delete(o)
            except InvalidRequestError:
                session.rollback()
        session.commit()

        redshift_engine.execute('drop table test__foo')
