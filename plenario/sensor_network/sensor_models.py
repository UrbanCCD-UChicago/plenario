from geoalchemy2 import Geometry
from sqlalchemy import Column, String, ForeignKey, Integer
from sqlalchemy.dialects.postgresql import ARRAY, JSON
from sqlalchemy.orm import relationship

from plenario.database import session, Base


class NetworkMeta(Base):
    __tablename__ = 'sensor_network_metadata'

    name = Column(String, primary_key=True)
    nodeMetadata = Column(JSON)
    featuresOfInterest = Column(ARRAY(String))
    nodes = relationship('NodeMeta', cascade='all, delete-orphan')

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
    featuresOfInterest = Column(ARRAY(String))
    procedures = Column(JSON)

    @classmethod
    def index(cls):
        nodes = session.query(cls)
        return [node.id for node in nodes]



