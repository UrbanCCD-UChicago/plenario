"""model_helpers: Just a collection of functions which perform common
interactions with the models."""

from sqlalchemy.exc import ProgrammingError

from plenario.database import app_engine


def table_exists(table_name):
    """Make an inexpensive query to the database. It the table does not exist,
    the query will cause a ProgrammingError.

    :param table_name: (string) table name
    :returns: (bool) true if the table exists, false otherwise"""

    try:
        app_engine.execute("select '{}'::regclass".format(table_name))
        return True
    except ProgrammingError:
        return False
