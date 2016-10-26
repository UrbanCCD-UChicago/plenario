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


class NodeMeta(Base):
    __tablename__ = 'sensor__node_metadata'

    id = Column(String, primary_key=True)
    sensor_network = Column(String, ForeignKey('sensor__network_metadata.name'), primary_key=True)
    location = Column(Geometry(geometry_type='POINT', srid=4326))
    sensors = relationship('SensorMeta', secondary='sensor__sensor_to_node')
    info = Column(JSONB)

    column_editable_list = ("sensors", "info")

    @staticmethod
    def index(network_name=None):
        nodes = session.query(NodeMeta).all()
        return [node.id.lower() for node in nodes if node.sensor_network == network_name or network_name is None]

    @staticmethod
    def nearest_neighbor_to(lng, lat):
        return knn(
            pk="id",
            geom="location",
            lng=lng,
            lat=lat,
            table="sensor__node_metadata",
            k=1
        )[0][0]

    def __repr__(self):
        return '<Node "{}">'.format(self.id)


class SensorMeta(Base):
    __tablename__ = 'sensor__sensors'

    name = Column(String, primary_key=True)
    observed_properties = Column(JSONB)
    info = Column(JSONB)

    @staticmethod
    def index(network_name=None):
        sensors = []
        for node in session.query(NodeMeta).all():
            if network_name is None or node.sensor_network.lower() == network_name.lower():
                for sensor in node.sensors:
                    sensors.append(sensor.name.lower())
        return list(set(sensors))

    def __repr__(self):
        return '<Sensor "{}">'.format(self.name)


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
                    for prop in sensor.observed_properties.itervalues():
                        features.append(prop.split('.')[0].lower())
        return list(set(features))

    def __repr__(self):
        return '<Feature "{}">'.format(self.name)
