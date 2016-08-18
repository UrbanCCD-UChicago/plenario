from geoalchemy2 import Geometry
from sqlalchemy import create_engine
from sqlalchemy import Column, String, ForeignKey, Table, MetaData
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

from plenario import db
from plenario.database import Base, session

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
    sensor_network = Column(String, ForeignKey('sensor__network_metadata.name'))
    location = Column(Geometry(geometry_type='POINT', srid=4326))
    sensors = relationship('Sensor', secondary='sensor__sensor_to_node')
    info = Column(JSONB)

    @staticmethod
    def index(network_name=None):
        nodes = session.query(NodeMeta).all()
        return [node.id for node in nodes if node.sensor_network == network_name or network_name is None]
    
    def __repr__(self):
        return '<Node "{}">' .format(self.id)

class FeatureOfInterest(Base):
    __tablename__ = 'sensor__features_of_interest'

    name = Column(String, primary_key=True)
    observed_properties = Column(JSONB)

    @staticmethod
    def index(network_name=None):
        features = []
        for node in session.query(NodeMeta).all():
            if node.sensor_network == network_name or network_name is None:
                for sensor in node.sensors:
                    for prop in sensor.observed_properties:
                        features.append(prop.split('.')[0])
        return list(set(features))


class Sensor(Base):
    __tablename__ = 'sensor__sensors'

    name = Column(String, primary_key=True)
    observed_properties = Column(ARRAY(String))
    info = Column(JSONB)

    @staticmethod
    def index(network_name=None):
        sensors = []
        for node in session.query(NodeMeta).all():
            if node.sensor_network == network_name or network_name is None:
                for sensor in node.sensors:
                    sensors.append(sensor.name)
        return list(set(sensors))

    def __repr__(self):
        return '<Sensor "{}">'.format(self.name)


if __name__ == "__main__":
    # Base.metadata.create_all(app_engine, extend_existing=True)
    Base.metadata.create_all()
