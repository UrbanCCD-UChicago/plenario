import os
from sqlalchemy import Column, Integer, String, Boolean, Table, Date, DateTime
from sqlalchemy.dialects.postgresql import TIMESTAMP, DOUBLE_PRECISION, TIME,\
    DATE
from geoalchemy2 import Geometry
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref
from fiona import prop_width, prop_type

from wopr.database import Base, app_engine as engine, Point

MetaTable = Table('meta_master', Base.metadata,
    autoload=True, autoload_with=engine)

MasterTable = Table('dat_master', Base.metadata,
    autoload=True, autoload_with=engine)

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

def sf_crime_table(name, metadata):
    table = Table(name, metadata,
            Column( 'id',           Integer                         ),
            Column( 'category',     String(length=50)               ),
            Column( 'description',  String(length=100)              ),
            Column( 'day_of_week',  String(length=10)               ),
            Column( 'date',         DATE                            ),
            Column( 'time',         TIME                            ),
            Column( 'pd_district',  String(length=20)               ),
            Column( 'resolution',   String(length=50)               ),
            Column( 'location_str', String(length=100)              ),
            Column( 'longitude',    DOUBLE_PRECISION(precision=53)  ),
            Column( 'latitude',     DOUBLE_PRECISION(precision=53)  ),
    extend_existing=True)
    return table

def map_esri_type(esri_type):
    """ Map esri type (extracted through fiona) to SQLAlchemy type """
    tl = esri_type.split(':')
    t = tl[0]
    l = int(tl[1]) if len(tl) > 1 else 80
    if      t == 'int':        return Integer
    elif    t == 'float':      return Float
    elif    t == 'double':     return Float(precision=15)
    elif    t == 'str':        return String(length=l)
    elif    t == 'date':       return Date
    elif    t == 'datetime':   return DateTime
    
def shp2table(name, metadata, schema, force_multipoly=False):
    """ Create a SQLAlchemy table schema from a shapefile schema opbtained through fiona
    """
    # Create a list of columns for the features' properties
    attr_list = []
    for p in schema['properties'].iteritems():
        attr_list.append(Column(p[0], map_esri_type(p[1])))
    # Create the geometry column
    geom_type = schema['geometry'].upper() if not force_multipoly \
        else 'MULTIPOLYGON'
    geom_col = Column('geom', Geometry(geom_type))
    attr_list.append(geom_col)
    table = Table(name, metadata, *attr_list, extend_existing=True)
    return table



