from geoalchemy2 import Geometry
from sqlalchemy import Column, String, ForeignKey, Table
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
from sqlalchemy.orm import relationship

from plenario.database import session, Base

sensor_to_foi = Table('sensor__sensor_to_foi', Base.metadata,
                      Column('sensor', String, ForeignKey('sensor__sensors.name')),
                      Column('foi', String, ForeignKey('sensor__features_of_interest.name'))
                      )

foi_to_network = Table('sensor__foi_to_network', Base.metadata,
                       Column('foi', String, ForeignKey('sensor__features_of_interest.name')),
                       Column('network', String, ForeignKey('sensor__network_metadata.name'))
                       )

network_to_sensor = Table('sensor__network_to_sensor', Base.metadata,
                          Column('network', String, ForeignKey('sensor__network_metadata.name')),
                          Column('sensor', String, ForeignKey('sensor__sensors.name'))
                          )


class NetworkMeta(Base):
    __tablename__ = 'sensor__network_metadata'

    name = Column(String, primary_key=True)
    nodes = relationship('NodeMeta')
    featuresOfInterest = relationship('FeatureOfInterest', secondary='sensor__foi_to_network',
                                      back_populates='sensorNetworks')
    sensors = relationship('Sensor', secondary='sensor__network_to_sensor',
                           back_populates='sensorNetworks')
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
    info = Column(JSONB)

    @staticmethod
    def index(network_name=None):
        nodes = session.query(NodeMeta).all()
        return [node.id for node in nodes if node.sensorNetwork == network_name or network_name is None]


class FeatureOfInterest(Base):
    __tablename__ = 'sensor__features_of_interest'

    name = Column(String, primary_key=True)
    observedProperties = Column(JSONB)
    sensors = relationship('Sensor', secondary='sensor__sensor_to_foi',
                           back_populates='featuresOfInterest')
    sensorNetworks = relationship('NetworkMeta', secondary='sensor__foi_to_network',
                                  back_populates='featuresOfInterest')

    @staticmethod
    def index(network_name=None):
        features = session.query(FeatureOfInterest).all()
        return [feature.name for feature in features if
                network_name in [network.name for network in feature.sensorNetworks] or network_name is None]


class Sensor(Base):
    __tablename__ = 'sensor__sensors'

    name = Column(String, primary_key=True)
    featuresOfInterest = relationship('FeatureOfInterest', secondary='sensor__sensor_to_foi',
                                      back_populates='sensors')
    properties = Column(ARRAY(String))
    sensorNetworks = relationship('NetworkMeta', secondary='sensor__network_to_sensor',
                                  back_populates='sensors')
    info = Column(JSONB)

    @staticmethod
    def index(network_name=None):
        sensors = session.query(Sensor).all()
        return [sensor.name for sensor in sensors if
                network_name in [network.name for network in sensor.sensorNetworks] or network_name is None]
