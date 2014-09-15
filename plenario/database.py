import os
import re
import psycopg2
from sqlalchemy import create_engine, types
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.declarative import declarative_base
from psycopg2.extensions import adapt, register_adapter, AsIs
import plenario.settings

app_engine = create_engine(plenario.settings.DATABASE_CONN, convert_unicode=True)
task_engine = create_engine(
    plenario.settings.DATABASE_CONN, 
    convert_unicode=True,
    poolclass=NullPool)

session = scoped_session(sessionmaker(bind=app_engine,
                                      autocommit=False,
                                      autoflush=False))

task_session = scoped_session(sessionmaker(bind=task_engine,
                                      autocommit=False,
                                      autoflush=False))
Base = declarative_base()
Base.query = session.query_property()

def init_db():
    import plenario.models
    from plenario.utils.weather import WeatherETL, WeatherStationsETL
    from plenario.utils.shapefile_helpers import PlenarioShapeETL
    from datetime import date

    print 'creating master, meta and user tables'
    Base.metadata.create_all(bind=app_engine)
    if plenario.settings.DEFAULT_USER:
        print 'creating default user %s' % plenario.settings.DEFAULT_USER['name']
        user = plenario.models.User(**plenario.settings.DEFAULT_USER)
        session.add(user)
        session.commit()

    print 'initializing NOAA weather stations'
    s = WeatherStationsETL()
    s.initialize()

    print 'initializing NOAA daily and hourly weather observations for %s/%s' % (date.now().month, date.now().year) 
    print 'this will take a few minutes ...'
    e = WeatherETL()
    e.initialize_month(date.now().year, date.now().month)

    print 'initializing and populating US Census blocks'
    print 'this will *also* take a few minutes ...'
    shp = PlenarioShapeETL(plenario.settings.CENSUS_BLOCKS)
    shp.add()