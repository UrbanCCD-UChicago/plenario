from datetime import datetime
from dateutil.parser import parse
from unittest import TestCase

from manage import init, uninstall
from plenario.database import postgres_session_context
from plenario.models.SensorNetwork import NetworkMeta, NodeMeta
from plenario.models.SensorNetwork import SensorMeta, FeatureMeta


def network(name, nodes):
    """nodes: [ NodeMeta, ... ]"""
    return NetworkMeta(name=name, nodes=nodes)


def node(name, network, sensors, location):
    """sensors: [ SensorMeta, ... ], location: geom"""
    return NodeMeta(
        id=name,
        sensor_network=network,
        sensors=sensors,
        location=location
    )


def sensor(name, properties):
    """properties: {'common_name': 'feature.property', ... }"""
    return SensorMeta(name=name, observed_properties=properties)


def feature(name, properties, networks):
    """properties: [{'type': 'redshift_type', 'name': 'property'}, ... ]"""
    return FeatureMeta(
        name=name,
        observed_properties=properties,
        networks=networks
    )


def observation(network, feature, properties, node, dt, sensor, values):
    insert = "insert into {network}__{feature} "
    insert += "(node_id, datetime, meta_id, sensor, {properties}) "
    insert += "values ('{node}', '{dt}', '0', '{sensor}', '{values}')"

    return insert.format(
        network=network,
        feature=feature,
        properties=', '.join(properties),
        node=node,
        dt=dt,
        sensor=sensor,
        values=', '.join(values)
    )


def fixtures():

    tmp0 = sensor('tmp0', {'temperature': 'temperature.temperature'})
    tmp1 = sensor('tmp1', {'howhot': 'temperature.temperature'})
    vec0 = sensor('vec0', {"vec_x": "vector.x", "vex_y": "vector.y"})
    sensors = [tmp0, tmp1, vec0]

    node0 = node('node0', 'aot0', sensors, '0101000020E6100000A4A7C821E2E755C07C48F8DEDFF04440')
    node1 = node('node1', 'aot0', sensors[1:], '0101000020E6100000A4A7C821E2E755C07C48F8DEDFF04440')
    nodes = [node0, node1]

    aot0 = network('aot0', nodes)
    aot1 = network('aot1', [])
    networks = [aot0, aot1]

    feature0 = feature('temperature', [{"type": "float", "name": "temperature"}], [aot0])
    feature1 = feature('vector', [{"type": "float", "name": "x"}, {"type": "float", "name": "y"}], [aot0])
    features = [feature0, feature1]

    with postgres_session_context() as session:
        for objects in [sensors, networks, features]:
            session.bulk_save_objects(objects)
            session.commit()

        for f in features:
            f.mirror()

        for i in range(1, 31):
            dt = parse("2017-01-{}".format(i))
            session.execute(observation('aot0', 'temperature', ['temperature'], 'node0', dt, 'tmp0', ['0']))
            session.execute(observation('aot0', 'temperature', ['temperature'], 'node1', dt, 'tmp0', ['20']))


class BaseTest(TestCase):

    @classmethod
    def setUpClass(cls):
        uninstall()
        init()
        fixtures()

