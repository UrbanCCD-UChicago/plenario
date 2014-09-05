import os
from sqlalchemy import Column, Integer, String, Boolean, Table, Date, DateTime, \
    Float, Numeric, Text, TypeDecorator, BigInteger
from sqlalchemy.dialects.postgresql import TIMESTAMP, DOUBLE_PRECISION, TIME,\
    DATE
from geoalchemy2 import Geometry
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref, deferred, synonym
from sqlalchemy.ext.hybrid import hybrid_property
from uuid import uuid4
from flask_bcrypt import Bcrypt
from plenario.database import session

from plenario.database import Base, app_engine as engine
#from plenario.auth import bcrypt
bcrypt = Bcrypt()

class MetaTable(Base):
    __tablename__ = 'meta_master'
    dataset_name = Column(String(100), nullable=False)
    human_name = Column(String(200), nullable=False)
    description = Column(Text)
    source_url = Column(String(100), primary_key=True)
    obs_from = Column(Date)
    obs_to = Column(Date)
    bbox = Column(Geometry('POLYGON', srid=4326))
    update_freq = Column(String(100), nullable=False)
    last_update = Column(DateTime)
    date_added = Column(DateTime)
    # Store the names of fields in source data
    business_key = Column(String, nullable=False)
    observed_date = Column(String, nullable=False)
    latitude = Column(String)
    longitude = Column(String)
    location = Column(String)

    def __repr__(self):
        return '<MetaTable %r (%r)>' % (self.human_name, self.dataset_name)

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

class MasterTable(Base):
    __tablename__ = 'dat_master'
    master_row_id = Column(BigInteger, primary_key=True)
    start_date = Column(TIMESTAMP)
    end_date = Column(TIMESTAMP)
    current_flag = Column(Boolean, default=True)
    location = Column(String(100))
    latitude = Column(DOUBLE_PRECISION(precision=53))
    longitude = Column(DOUBLE_PRECISION(precision=53))
    obs_date = Column(Date, index=True)
    obs_ts = Column(TIMESTAMP, default=None)
    weather_station_id = Column(String(50))
    geotag2 = Column(String(50))
    geotag3 = Column(String(50))
    dataset_name = Column(String(100))
    dataset_row_id = Column(Integer)
    location_geom = Column(Geometry('POINT', srid=4326))

    def __repr__(self):
        return '<Master %r (%r)>' % (self.dataset_row_id, self.dataset_name)

def get_uuid():
    return unicode(uuid4())

class User(Base):
    __tablename__ = 'plenario_user'
    id = Column(String(36), default=get_uuid, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    email = Column(String, nullable=False)
    _password = Column('password', String(60), nullable=False)

    def _get_password(self):
        return self._password

    def _set_password(self, value):
        self._password = bcrypt.generate_password_hash(value)

    password = property(_get_password, _set_password)
    password = synonym('_password', descriptor=password)

    def __init__(self, name, password, email):
        self.name = name
        self.password = password
        self.email = email

    @classmethod
    def get_by_username(cls, name):
        return session.query(cls).filter(cls.name == name).first()

    @classmethod
    def check_password(cls, name, value):
        user = cls.get_by_username(name)
        if not user:
            return False
        return bcrypt.check_password_hash(user.password, value)

    def is_authenticated(self):
        return True
    
    def is_active(self):
        return True

    def is_anonymous(self):
        return False

    def get_id(self):
        return self.id

def crime_table(name, metadata):
    table = Table(name, metadata,
            Column('id', Integer),
            Column('case_number', String(length=10)),
            Column('orig_date', TIMESTAMP),
            Column('block', String(length=50)),
            Column('iucr', String(length=10)),
            Column('primary_type', String(length=100)),
            Column('description', String(length=100)),
            Column('location_description', String(length=50)),
            Column('arrest', Boolean),
            Column('domestic', Boolean),
            Column('beat', String(length=10)),
            Column('district', String(length=5)),
            Column('ward', Integer),
            Column('community_area', String(length=10)),
            Column('fbi_code', String(length=10)),
            Column('x_coordinate', Integer, nullable=True),
            Column('y_coordinate', Integer, nullable=True),
            Column('year', Integer),
            Column('updated_on', TIMESTAMP, default=None),
            Column('latitude', DOUBLE_PRECISION(precision=53)),
            Column('longitude', DOUBLE_PRECISION(precision=53)),
            Column('location', Point),
    extend_existing=True)
    return table

def map_esri_type(esri_type):
    """ Map esri type (extracted through fiona) to SQLAlchemy type. """
    tl = esri_type.split(':')
    t = tl[0]
    l = tl[1] if len(tl) > 1 else None
    if      t == 'int':        return Integer
    elif    t == 'double':     return Float(precision=15)
    elif    t == 'str':        return String(length=int(l) if l else 80)
    elif    t == 'date':       return Date
    elif    t == 'datetime':   return DateTime
    elif    t == 'float':
        if not l:              return Float
        else:
            ps = l.split('.')
            return Numeric(int(ps[0]), int(ps[1]))
            
def shp2table(name, metadata, schema, force_multipoly=False):
    """ Create a SQLAlchemy table schema from a shapefile schema
        obtained through fiona.
    """
    # Create a list of columns for the features' properties
    attr_list = []
    for p in schema['properties'].iteritems():
        attr_list.append(Column(p[0].lower(), map_esri_type(p[1])))
    # Create the geometry column
    geom_type = schema['geometry'].upper() if not force_multipoly \
        else 'MULTIPOLYGON'
    geom_col = Column('geom', Geometry(geom_type, srid=4326))
    attr_list.append(geom_col)
    table = Table(name, metadata, *attr_list, extend_existing=True)
    return table



