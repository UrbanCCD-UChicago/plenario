import subprocess
from contextlib import contextmanager
from logging import getLogger

from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session

from plenario.settings import DATABASE_CONN, REDSHIFT_CONN


logger = getLogger(__name__)


postgres_engine = create_engine(DATABASE_CONN)
postgres_session = scoped_session(sessionmaker(bind=postgres_engine))
postgres_base = declarative_base(bind=postgres_engine)
postgres_base.query = postgres_session.query_property()

redshift_engine = create_engine(REDSHIFT_CONN, max_overflow=-1)
redshift_session = scoped_session(sessionmaker(bind=redshift_engine, autocommit=True))
redshift_base = declarative_base(bind=redshift_engine)
redshift_base.query = redshift_session.query_property()


def create_database(bind: Engine, database: str) -> None:
    """Setup a database (schema) in postgresql.
    """
    logger.info('[plenario] Create database %s' % database)
    connection = bind.connect()
    connection.execute('commit')
    connection.execute('create database %s' % database)
    connection.close()


def drop_database(bind: Engine, database: str) -> None:
    """Drop a database (schema) in postgresql.
    """
    logger.info('[plenario] Drop database %s' % database)
    connection = bind.connect()
    connection.execute('commit')
    connection.execute('drop database %s' % database)
    connection.close()


def create_extension(bind: Engine, extension: str) -> None:
    """Setup an extension in postgresql.
    """
    logger.info('[plenario] Create extension %s' % extension)
    connection = bind.connect()
    connection.execute('create extension %s' % extension)
    connection.close()


def psql(path: str) -> None:
    """Use psql to run a file at some path.
    """
    logger.info('[plenario] Psql file %s' % path)
    command = 'psql {} -f {}'.format(DATABASE_CONN, path)
    subprocess.check_call(command, shell=True)


@contextmanager
def postgres_session_context():
    """A helper method for keeping the state of an connection with the database
    separate from the work being done, and ensuring that the session is always
    cleaned up after use.
    """
    logger.info('Begin.')
    transactional_session = postgres_session()
    try:
        yield transactional_session
        transactional_session.commit()
    except:
        transactional_session.rollback()
        logger.error('Postgres transaction failed.', exc_info=True)
        raise
    finally:
        transactional_session.close()
    logger.info('End.')


@contextmanager
def redshift_session_context():
    """A helper method for keeping the state of an connection with the database
    separate from the work being done, and ensuring that the session is always
    cleaned up after use.
    """
    logger.info('Begin.')
    transactional_session = redshift_session()
    try:
        yield transactional_session
        transactional_session.commit()
    except:
        transactional_session.rollback()
        logger.error('Redshift transaction failed.', exc_info=True)
        raise
    finally:
        transactional_session.close()
    logger.info('End.')
