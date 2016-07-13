import json
import sqlalchemy as sa

from collections import namedtuple
from datetime import datetime
from flask_bcrypt import Bcrypt
from geoalchemy2 import Geometry
from hashlib import md5
from itertools import groupby
from operator import itemgetter
from sqlalchemy import Column, String, Boolean, Date, DateTime, Text, func, ForeignKey
from sqlalchemy import Table, select, Integer
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, JSON
from sqlalchemy.orm import synonym, relationship
from sqlalchemy.types import NullType
from uuid import uuid4

from plenario.database import session, Base
from plenario.utils.helpers import get_size_in_degrees, slugify

bcrypt = Bcrypt()



class NetworkMeta(Base):
    __tablename__ = 'sensor_network_metadata'

    name = Column(String, primary_key=True)
    nodeMetadata = Column(JSON)
    featuresOfInterest = Column(ARRAY(String))
    nodes = relationship('NodeMeta', backref='network', cascade='all, delete-orphan')


class NodeMeta(Base):
    __tablename__ = 'sensor_node_metadata'

    id = Column(String, primary_key=True)
    sensorNetwork = Column(String, ForeignKey('sensor_network_metadata.name'))
    location = Column(Geometry(geometry_type='POINT'))
    version = Column(Integer)
    featuresOfInterest = Column(ARRAY(String))
    procedures = Column(JSON)

