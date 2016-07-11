import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.declarative import declarative_base

from plenario.settings import DATABASE_CONN

if os.environ.get('WORKER'):
    app_engine = create_engine(DATABASE_CONN, convert_unicode=True, pool_size=5)
else:
    app_engine = create_engine(DATABASE_CONN, convert_unicode=True, pool_size=2)

session = scoped_session(sessionmaker(bind=app_engine,
                                      autocommit=False,
                                      autoflush=False, expire_on_commit=False))
Base = declarative_base(bind=app_engine)
Base.query = session.query_property()
