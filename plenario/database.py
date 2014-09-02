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
    Base.metadata.create_all(bind=app_engine)
    if plenario.settings.DEFAULT_USER:
        user = plenario.models.User(**plenario.settings.DEFAULT_USER)
        session.add(user)
        session.commit()
