from geoalchemy2 import Geometry
from sqlalchemy import Column, String, ForeignKey, Integer, Table
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from plenario.database import session, Base


association_table = Table('association', Base.metadata,
                          Column('foi_name', String, ForeignKey('sensor_features_of_interest.name')),
                          Column('node_id', String, ForeignKey('sensor_node_metadata.id'))
                          )


class NetworkMeta(Base):
    __tablename__ = 'sensor_network_metadata'

    name = Column(String, primary_key=True)
    nodeMetadata = Column(JSONB)
    nodes = relationship('NodeMeta')
    featuresOfInterest = relationship('FeatureOfInterest')

    @classmethod
    def index(cls):
        networks = session.query(cls)
        return [network.name for network in networks]


class NodeMeta(Base):
    __tablename__ = 'sensor_node_metadata'

    id = Column(String, primary_key=True)
    sensorNetwork = Column(String, ForeignKey('sensor_network_metadata.name'))
    location = Column(Geometry(geometry_type='POINT', srid=4326))
    version = Column(Integer)
    procedures = Column(JSONB)
    featuresOfInterest = relationship('FeatureOfInterest', secondary=association_table)

    @classmethod
    def index(cls):
        nodes = session.query(cls)
        return [node.id for node in nodes]


class FeatureOfInterest(Base):
    __tablename__ = 'sensor_features_of_interest'

    name = Column(String, primary_key=True)
    sensorNetwork = Column(String, ForeignKey('sensor_network_metadata.name'))
    observedProperties = Column(JSONB)

    @classmethod
    def index(cls):
        features = session.query(cls)
        return [feature.name for feature in features]