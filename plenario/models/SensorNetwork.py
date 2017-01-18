from geoalchemy2 import Geometry
from sqlalchemy import Table, String, Column, ForeignKey, ForeignKeyConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from plenario.database import Base, session
from plenario.utils.model_helpers import knn

sensor_to_node = Table('sensor__sensor_to_node',
                       Base.metadata,
                       Column('sensor', String, ForeignKey('sensor__sensors.name')),
                       Column('network', String),
                       Column('node', String),
                       ForeignKeyConstraint(['network', 'node'],
                                            ['sensor__node_metadata.sensor_network', 'sensor__node_metadata.id'])
                       )


class NetworkMeta(Base):
    __tablename__ = 'sensor__network_metadata'

    name = Column(String, primary_key=True)
    nodes = relationship('NodeMeta')
    info = Column(JSONB)

    @staticmethod
    def index():
        networks = session.query(NetworkMeta)
        return [network.name.lower() for network in networks]

    def __repr__(self):
        return '<Network "{}">'.format(self.name)

    def tree(self):
        return {n.id: n.tree() for n in self.nodes}

    def sensors(self):

        keys = []
        for sensor in self.tree().values():
            keys += sensor

        return keys

    def features(self):

        keys = []
        for sensor in self.tree().values():
            for feature in sensor.values():
                keys += feature.keys()

        return set([k.split(".")[0] for k in keys])


class NodeMeta(Base):
    __tablename__ = 'sensor__node_metadata'

    id = Column(String, primary_key=True)
    sensor_network = Column(String, ForeignKey('sensor__network_metadata.name'), primary_key=True)
    location = Column(Geometry(geometry_type='POINT', srid=4326))
    sensors = relationship('SensorMeta', secondary='sensor__sensor_to_node')
    info = Column(JSONB)

    column_editable_list = ("sensors", "info")

    @staticmethod
    def all(network_name):
        query = NodeMeta.query.filter(NodeMeta.sensor_network == network_name)
        return query.all()

    @staticmethod
    def index(network_name):
        return [node.id for node in NodeMeta.all(network_name)]

    @staticmethod
    def nearest_neighbor_to(lng, lat, network, features):
        sensors = SensorMeta.get_sensors_from_features(features)
        return knn(
            lng=lng,
            lat=lat,
            network=network,
            sensors=sensors,
            k=10
        )

    @staticmethod
    def get_nodes_from_sensors(network, sensors):
        rp = session.execute("""
            select distinct id
            from sensor__node_metadata
            inner join sensor__sensor_to_node
            on id = node
            where sensor = any('{0}'::text[])
            and network = '{1}'
        """.format("{" + ",".join(sensors) + "}", network))
        return [row.id for row in rp]

    def __repr__(self):
        return '<Node "{}">'.format(self.id)

    def tree(self):
        return {s.name: s.tree() for s in self.sensors}


class SensorMeta(Base):
    __tablename__ = 'sensor__sensors'

    name = Column(String, primary_key=True)
    observed_properties = Column(JSONB)
    info = Column(JSONB)

    @staticmethod
    def get_sensors_from_features(features):
        full_features = []
        for feature in features:
            if len(feature.split(".")) == 1:
                full_features += FeatureMeta.properties_of(feature)
        features = set(features + full_features)

        rp = session.execute("""
            select distinct name
            from sensor__sensors_view
            where invert ?| '{}'
        """.format("{" + ",".join(features) + "}"))

        return [row.name for row in rp]

    def __repr__(self):
        return '<Sensor "{}">'.format(self.name)

    def tree(self):
        return {v: k for k, v in self.observed_properties.items()}


class FeatureMeta(Base):
    __tablename__ = 'sensor__features_of_interest'

    name = Column(String, primary_key=True)
    observed_properties = Column(JSONB)

    @staticmethod
    def index(network_name=None):
        features = []
        for node in session.query(NodeMeta).all():
            if network_name is None or node.sensor_network.lower() == network_name.lower():
                for sensor in node.sensors:
                    for prop in sensor.observed_properties.values():
                        features.append(prop.split('.')[0].lower())
        return list(set(features))

    @staticmethod
    def properties_of(feature):
        query = session.query(FeatureMeta.observed_properties).filter(
            FeatureMeta.name == feature)
        return [feature + "." + prop["name"] for prop in query.first().observed_properties]

    def __repr__(self):
        return '<Feature "{}">'.format(self.name)
