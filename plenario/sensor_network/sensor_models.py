from geoalchemy2 import Geometry
from sqlalchemy import Column, String, ForeignKey, Table
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
from sqlalchemy.orm import relationship

from plenario.database import session, Base

sensor_to_node = Table('sensor__sensor_to_node', Base.metadata,
                      Column('sensor', String, ForeignKey('sensor__sensors.name')),
                      Column('node', String, ForeignKey('sensor__node_metadata.id'))
                      )


class NetworkMeta(Base):
    __tablename__ = 'sensor__network_metadata'

    name = Column(String, primary_key=True)
    nodes = relationship('NodeMeta')
    info = Column(JSONB)

    @staticmethod
    def index():
        networks = session.query(NetworkMeta)
        return [network.name for network in networks]


class NodeMeta(Base):
    __tablename__ = 'sensor__node_metadata'

    id = Column(String, primary_key=True)
    sensorNetwork = Column(String, ForeignKey('sensor__network_metadata.name'))
    location = Column(Geometry(geometry_type='POINT', srid=4326))
    sensors = relationship('Sensor', secondary='sensor__sensor_to_node')
    info = Column(JSONB)

    @staticmethod
    def index(network_name=None):
        nodes = session.query(NodeMeta).all()
        return [node.id for node in nodes if node.sensorNetwork == network_name or network_name is None]


class FeatureOfInterest(Base):
    __tablename__ = 'sensor__features_of_interest'

    name = Column(String, primary_key=True)
    observedProperties = Column(JSONB)

    @staticmethod
    def index(network_name=None):
        features = session.query(FeatureOfInterest).all()
        return [feature.name for feature in features if
                network_name in [network.name for network in feature.sensorNetworks] or network_name is None]


class Sensor(Base):
    __tablename__ = 'sensor__sensors'

    name = Column(String, primary_key=True)
    properties = Column(ARRAY(String))
    info = Column(JSONB)

    @staticmethod
    def index(network_name=None):
        sensors = session.query(Sensor).all()
        return [sensor.name for sensor in sensors if
                network_name in [network.name for network in sensor.sensorNetworks] or network_name is None]
