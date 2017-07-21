import logging

from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError

from plenario.database import redshift_engine


logger = logging.getLogger(__name__)


def create_foi_table(foi_name, properties):
    """Create a new foi table
    
    :param foi_name: name of feature
    :param properties: list of {'name': name, 'type': type} dictionaries 
    """
    template = """
        CREATE TABLE {table} (
          "node_id" VARCHAR NOT NULL,
          "datetime" TIMESTAMP WITHOUT TIME ZONE NOT NULL,
          "meta_id" DOUBLE PRECISION NOT NULL,
          "sensor" VARCHAR NOT NULL,
          {props},
          PRIMARY KEY ("node_id", "datetime")
        )
        DISTKEY(datetime)
        SORTKEY(datetime);
    """
    kwargs = {
        'table': foi_name,
        'props': ', '.join('"{}" {}'.format(p['name'], p['type']) for p in properties)
    }

    operation = template.format(**kwargs)
    logger.info(operation)
    redshift_engine.execute(text(operation))


def table_exists(table_name):
    """Make an inexpensive query to the database. It the table does not exist,
    the query will cause a ProgrammingError.

    :param table_name: (string) table name
    :returns (bool) true if the table exists, false otherwise
    """
    try:
        redshift_engine.execute("select '{}'::regclass".format(table_name))
        return True
    except ProgrammingError:
        return False
