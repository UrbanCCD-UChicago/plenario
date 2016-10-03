import os

from plenario.sensor_network.sensor_models import *

from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError, ProgrammingError


class Fixtures:

    def _run_with_connection(self, query):
        conn = self.engine.connect()
        try:
            print query
            conn.execute("commit")
            conn.execute(query)
        except ProgrammingError as err:
            print err
        finally:
            conn.close()

    def __init__(self):
        self.user = os.environ["DB_USER"] = "postgres"
        self.password = os.environ["DB_PASSWORD"] = "password"
        self.host = os.environ["DB_HOST"] = "localhost"
        self.port = os.environ["DB_PORT"] = "5432"
        self.dbname = os.environ["DB_NAME"] = "sensor_meta_test"

        self.user = os.environ["RS_USER"] = "postgres"
        self.password = os.environ["RS_PASSWORD"] = "password"
        self.host = os.environ["RS_HOST"] = "localhost"
        self.port = os.environ["RS_PORT"] = "5432"
        self.dbname = os.environ["RS_NAME"] = "sensor_obs_test"

        self.base_db_url = "postgresql://{}:{}@{}:{}".format(self.user, self.password, self.host, self.port)
        self.engine = create_engine(self.base_db_url)
        self.pg_engine = None
        self.rs_engine = None

    def setup_databases(self):
        self._run_with_connection("create database sensor_meta_test")
        self._run_with_connection("create database sensor_obs_test")
        self.rs_engine = create_engine(self.base_db_url + "/sensor_obs_test")
        self.pg_engine = create_engine(self.base_db_url + "/sensor_meta_test")
        self.pg_engine.execute("create extension postgis")

    def generate_sensor_network_meta_tables(self):
        print "Creating sensor network tables for {} ..." .format(self.engine)
        Base.metadata.create_all(bind=self.pg_engine)

    def drop_databases(self):
        self._run_with_connection("drop database sensor_meta_test")
        self._run_with_connection("drop database sensor_obs_test")

    def generate_mock_metadata(self):
        session.configure(bind=self.pg_engine)

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
            location="0101000020E6100000A4A7C821E2E755C07C48F8DEDFF04440",
        )

        network = NetworkMeta(
            name="test_network",
            nodes=[node],
        )

        network_02 = NetworkMeta(
            name="test_network_other",
        )

        for obj in [feature_01, feature_02, network, network_02, node]:
            try:
                print "INSERT {} with {}".format(obj, session.get_bind())
                session.add(obj)
                session.commit()
            except IntegrityError as err:
                print str(err) + "\n"
                session.rollback()

    def generate_mock_features_of_interest(self):
        from plenario.sensor_network.redshift_ops import create_foi_table

        create_foi_table("vector", [{"type": "float", "name": "x"}, {"type": "float", "name": "y"}])
        self.rs_engine.execute("insert into vector ()")
