# FIXME(heyzoos)
# This test needs to rely on fixtures it creates for itself, not on the
# byproducts of other tests (which is what it used to do).
#
# import unittest
#
# from sqlalchemy.exc import ProgrammingError, IntegrityError, InvalidRequestError
#
# from plenario.database import redshift_engine, redshift_session
# from plenario.database import postgres_session as postgres_session
# from plenario.models.SensorNetwork import NetworkMeta, FeatureMeta
#
# from tests.test_sensor_network.fixtures import Fixtures
# from tests.fixtures.base_test import BasePlenarioTest
#
#
# session = postgres_session()
# objects = []
#
#
# def redshift_table_exists(table_name):
#     """Make an inexpensive query to the database. It the table does not exist,
#     the query will cause a ProgrammingError.
#
#     :param table_name: (string) table name
#     :returns (bool) true if the table exists, false otherwise"""
#
#     try:
#         redshift_engine.execute("select '{}'::regclass".format(table_name))
#         return True
#     except ProgrammingError:
#         return False
#
#
# class TestFeatureMeta(unittest.TestCase):
#
#     @classmethod
#     def setUpClass(cls):
#         cls.fixtures = Fixtures()
#         BasePlenarioTest.setUpClass()
#         cls.fixtures.generate_sensor_network_meta_tables()
#         cls.fixtures.generate_mock_observations()
#         cls.fixtures.generate_mock_metadata()
#
#     def setUp(self):
#         network = NetworkMeta(name='test')
#         feature = FeatureMeta(
#             name='foo',
#             networks=[network],
#             observed_properties=[{
#                 'name': 'bar',
#                 'type': 'float'
#             }, {
#                 'name': 'baz',
#                 'type': 'float'
#             }]
#         )
#
#         objects.append(network)
#         objects.append(feature)
#
#         try:
#             session.add(network)
#             session.add(feature)
#             session.commit()
#         except IntegrityError:
#             session.rollback()
#
#     @classmethod
#     def tearDownClass(cls):
#         redshift_session.close()
#         redshift_engine.dispose()
#         BasePlenarioTest.tearDownClass()
#
#     def test_feature_meta_mirror(self):
#         """Redshift table is created for a call to mirror?"""
#
#         feature = FeatureMeta.query.get('foo')
#         feature.mirror()
#         self.assert_(redshift_table_exists('test__foo'))
