import os, sys
sys.path.insert(0, os.path.abspath('../..'))

from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError

from plenario.database import redshift_engine


def create_foi_table(foi_name, properties):
    """Create a new foi table

       :param foi_name: name of feature
       :param properties: list of {'name': name, 'type': type} dictionaries """

    op = ('CREATE TABLE {} ('
          '"node_id" VARCHAR NOT NULL, '
          'datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL, '
          '"meta_id" DOUBLE PRECISION NOT NULL, '
          '"sensor" VARCHAR NOT NULL, ').format(foi_name)
    for prop in properties:
        op = (op + '"{}" {}, '.format(prop['name'], prop['type']))
    op = (op + ('PRIMARY KEY ("node_id", datetime)) '
          'DISTKEY(datetime) SORTKEY(datetime);'))
    print(op)
    redshift_engine.execute(text(op))


def add_column(network_name, column_name, column_type):
    op = text('ALTER TABLE {} '
              'ADD COLUMN {} {} '
              'DEFAULT NULL'.format(network_name, column_name, column_type))
    redshift_engine.execute(op)


def insert_observation(foi_name, nodeid, datetime, sensor,
                       values, procedures):
    """Inserts sensor readings

          :param values: list of observed property values in order
          :param procedures: integer procedure identifier """

    op = ('INSERT INTO {} '
          'VALUES ({}, {}, {}'
          .format(foi_name.lower(), repr(nodeid), repr(datetime), repr(sensor)))
    for val in values:
        op = (op + ', {}'.format(val))
    op = (op + ', {});'.format(str(procedures)))
    print(op)
    op = text(op)
    redshift_engine.execute(op)


def table_exists(table_name):
    """Make an inexpensive query to the database. It the table does not exist,
    the query will cause a ProgrammingError.

    :param table_name: (string) table name
    :returns (bool) true if the table exists, false otherwise"""

    try:
        redshift_engine.execute("select '{}'::regclass".format(table_name))
        return True
    except ProgrammingError:
        return False