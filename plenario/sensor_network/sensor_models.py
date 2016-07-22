from geoalchemy2 import Geometry
from sqlalchemy import Column, String, ForeignKey
from sqlalchemy import *
from sqlalchemy.dialects.postgresql import ARRAY, JSON
from sqlalchemy.orm import relationship

from plenario.database import session, Base
from plenario.database import redshift_engine, redshift_Base


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


class Observation(redshift_Base):
    __tablename__ = 'observations'

    node_id = Column(String, primary_key=True)
    datetime = Column(DateTime, primary_key=True)
    temperature_temperature = Column(Numeric)
    atmosphericPressure_atmosphericPressure = Column(Numeric)
    relativeHumidity_relativeHumidity = Column(Numeric)
    lightIntensity_lightIntensity = Column(Numeric)
    acceleration_X = Column(Numeric)
    acceleration_Y = Column(Numeric)
    acceleration_Z = Column(Numeric)
    instantaneousSoundSample_instantaneousSoundSample = Column(Numeric)
    magneticFieldIntensity_X = Column(Numeric)
    magneticFieldIntensity_Y = Column(Numeric)
    magneticFieldIntensity_Z = Column(Numeric)
    concentrationOf_SO2 = Column(Numeric)
    concentrationOf_H2S = Column(Numeric)
    concentrationOf_O3 = Column(Numeric)
    concentrationOf_NO2 = Column(Numeric)
    concentrationOf_CO = Column(Numeric)
    concentrationOf_reducingGases = Column(Numeric)
    concentrationOf_oxidizingGases = Column(Numeric)
    particulateMatter_PM1 = Column(Numeric)
    particulateMatter_PM2_5 = Column(Numeric)
    particulateMatter_PM10 = Column(Numeric)



