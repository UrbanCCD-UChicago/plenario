import os
import re
import psycopg2
from sqlalchemy.exc import IntegrityError
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

def init_db(no_create=False):
    import plenario.models
    from plenario.utils.weather import WeatherETL, WeatherStationsETL
    from plenario.utils.shapefile_helpers import PlenarioShapeETL
    import datetime

    if no_create:
        return
    
    print 'creating master, meta and user tables'
    Base.metadata.create_all(bind=app_engine)
    if plenario.settings.DEFAULT_USER:
        print 'creating default user %s' % plenario.settings.DEFAULT_USER['name']
        user = plenario.models.User(**plenario.settings.DEFAULT_USER)
        session.add(user)
        try: 
            session.commit()
        except IntegrityError:
            pass
    
        
    print 'initializing NOAA weather stations'
    s = WeatherStationsETL()
    s.initialize()

    print 'initializing NOAA daily and hourly weather observations for %s/%s' % (datetime.datetime.now().month, datetime.datetime.now().year) 
    print 'this will take a few minutes ...'
    e = WeatherETL()
    e.initialize_month(datetime.datetime.now().year, datetime.datetime.now().month)

    print 'initializing and populating US Census blocks'
    print 'this will *also* take a few minutes ...'
    shp = PlenarioShapeETL(plenario.settings.CENSUS_BLOCKS)
    shp.add()
    
