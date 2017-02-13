import unittest

from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import sessionmaker

from plenario import create_app
from plenario.database import Base, psql
from plenario.models.SensorNetwork import NetworkMeta, NodeMeta
from plenario.models.SensorNetwork import SensorMeta, FeatureMeta

from .fixtures import Fixtures

from manage import init


postgres_uri = 'postgresql://postgres:password@localhost:5432'

engine = create_engine(postgres_uri)
connection = engine.connect()


class TestNearest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        connection.execute('commit')
        try:
            connection.execute('drop database plenario_test')
        except ProgrammingError:
            pass
        connection.execute('commit')
        connection.execute('create database plenario_test')
        cls.engine = create_engine(postgres_uri + '/plenario_test')
        cls.session = sessionmaker(bind=cls.engine)()
        cls.connection = cls.engine.connect()
        cls.connection.execute('create extension postgis')

        Base.metadata.create_all(bind=cls.engine)
        init()

        temperature = FeatureMeta(name="temperature", observed_properties=[{"type": "float", "name": "temperature"}])
        vector = FeatureMeta(name="vector", observed_properties=[{"type": "float", "name": "x"}, {"type": "float", "name": "y"}])
        tmp000 = SensorMeta(name="tmp000", observed_properties={"howhot": "temperature.temperature"})
        vec000 = SensorMeta(name="vec000", observed_properties={"vec_x": "vector.x", "vex_y": "vector.y"})
        node_a = NodeMeta(id="node_a", sensor_network="array_of_things__test", sensors=[tmp000, vec000], location="0101000020E6100000A4A7C821E2E755C07C48F8DEDFF04440")
        node_b = NodeMeta(id="node_b", sensor_network="array_of_things__test", sensors=[tmp000, vec000], location="0101000020E6100000A4A7C821E2E755C07C48F8DEDFF04440")
        node_c = NodeMeta(id="node_c", sensor_network="array_of_things__test", sensors=[tmp000, vec000], location="0101000020E6100000A4A7C821E2E755C07C48F8DEDFF04440")
        network = NetworkMeta(name="array_of_things_test", nodes=[node_a, node_b, node_c])

        for obj in [temperature, vector, tmp000, vec000, node_a, node_b, node_c, network]:
            cls.session.add(obj)
        cls.session.commit()

        fixtures = Fixtures()
        fixtures.rs_engine = cls.engine
        fixtures._create_foi_table({"name": "array_of_things_test__vector", "properties": [{"name": "x", "type": "float"}, {"name": "y", "type": "float"}]})
        fixtures._create_foi_table({"name": "array_of_things_test__temperature", "properties": [{"name": "temperature", "type": "float"}]})

        cls.app = create_app().test_client()

    def test_nearest_good_request(self):

        request = '/v1/api/sensor-networks/array_of_things_test/nearest'
        request += '?feature=temperature&lat=41&lng=-80'
        resp = self.app.get(request)
        self.assertEqual(resp.status_code, 200)

    def test_nearest_bad_feature(self):

        request = '/v1/api/sensor-networks/array_of_things_test/nearest'
        request += '?feature=tmprtr&lat=41&lng=-80'
        resp = self.app.get(request)
        self.assertEqual(resp.status_code, 400)

    def test_nearest_malformed_request(self):

        request = '/v1/api/sensor-networks/array_of_things_test/nearest'
        request += '?feature=temperature&lat=41'
        resp = self.app.get(request)
        self.assertEqual(resp.status_code, 400)
