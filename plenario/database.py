from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.declarative import declarative_base

from plenario.settings import DATABASE_CONN, REDSHIFT_CONN


app_engine = create_engine(DATABASE_CONN, convert_unicode=True)

session = scoped_session(sessionmaker(bind=app_engine,
                                      autocommit=False,
                                      autoflush=False, expire_on_commit=False))
Base = declarative_base(bind=app_engine)
Base.query = session.query_property()

# Redshift connection setup

redshift_engine = create_engine(REDSHIFT_CONN, convert_unicode=True)

redshift_session = scoped_session(sessionmaker(bind=redshift_engine,
                                      autocommit=False,
                                      autoflush=False, expire_on_commit=False))
redshift_Base = declarative_base(bind=redshift_engine)
redshift_Base.query = session.query_property()