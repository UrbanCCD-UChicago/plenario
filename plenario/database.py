from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.declarative import declarative_base

from plenario.settings import DATABASE_CONN


app_engine = create_engine(DATABASE_CONN, convert_unicode=True)
task_engine = create_engine(
    DATABASE_CONN,
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
