import os

from plenario.sensor_network.sensor_models import *

from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError, ProgrammingError


class Fixtures:

    def __init__(self):
        self.user = os.environ["DB_USER"] = "postgres"
        self.password = os.environ["DB_PASSWORD"] = "password"
        self.host = os.environ["DB_HOST"] = "localhost"
        self.port = os.environ["DB_PORT"] = "5432"
        self.dbname = os.environ["DB_NAME"] = "sensor_meta_test"

        self.url = "postgresql://{}:{}@{}:{}".format(self.user, self.password, self.host, self.port)
        self.engine = create_engine(self.url)

    def setup_environment(self):
        print "Creating database sensor_meta_test ..."
        conn = self.engine.connect()
        try:
            conn.execute("commit")
            conn.execute("create database sensor_meta_test")
        except ProgrammingError as err:
            print err
        finally:
            conn.close()

        print "Connect to database sensor_meta_test ..."
        self.engine = create_engine(self.url + "/sensor_meta_test")
        try:
            self.engine.execute("create extension postgis")
        except ProgrammingError as err:
            print err

    def generate_sensor_network_meta_tables(self):
        print "Creating sensor network tables for {} ..." .format(self.engine)
        Base.metadata.create_all(bind=self.engine)

    def clear_sensor_network_meta_tables(self):
        try:
            from init_db import sensor_meta_table_names
            for table_name in sensor_meta_table_names:
                self.engine.execute("drop table {}".format(table_name))
        except ProgrammingError as err:
            print err

    def generate_mock_metadata(self):
        print "Creating mock metadata for {} ...".format(self.engine)
        session.configure(bind=self.engine)

        feature_01 = FeatureOfInterest(
            name="temperature",
            observed_properties=[{"type": "float", "name": "temperature"}]
        )

        feature_02 = FeatureOfInterest(
            name="vector",
            observed_properties=[{"type": "float", "name": "x"}, {"type": "float", "name": "y"}]
        )

        sensor_01 = Sensor(
            name="test_sensor_01",
            observed_properties={"howhot": "temperature.temperature"}
        )

        sensor_02 = Sensor(
            name="test_sensor_02",
            observed_properties={"vec_x": "vector.x", "vex_y": "vector.y"}
        )

        node = NodeMeta(
            id="test_node",
            sensor_network="test_network",
            sensors=[sensor_01, sensor_02],
            location="0101000020e61000000e4faf9465f0444055c1a8a44ee855c0",
        )

        network = NetworkMeta(
            name="test_network",
            nodes=[node],
        )

        network_02 = NetworkMeta(
            name="test_network_other",
        )

        for obj in [feature_01, feature_02, node, network, network_02]:
            try:
                print "INSERT {} with {}".format(obj, session.get_bind())
                session.add(obj)
                session.commit()
            except IntegrityError as err:
                print str(err) + "\n"
                session.rollback()

    def teardown_environment(self):
        self.engine = create_engine(self.url)
        print "Dropping sensor_meta_test for {} ...".format(self.engine)
        self.engine.execute("drop database sensor_meta_test")
