from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.declarative import declarative_base

from plenario.settings import DATABASE_CONN


app_engine = create_engine(DATABASE_CONN, convert_unicode=True)

session = scoped_session(sessionmaker(bind=app_engine,
                                      autocommit=False,
                                      autoflush=False))
Base = declarative_base(bind=app_engine)
Base.query = session.query_property()
