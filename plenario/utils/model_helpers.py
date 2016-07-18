"""model_helpers: Just a collection of functions which perform common
interactions with the models."""

from sqlalchemy.exc import ProgrammingError
from plenario.database import app_engine, session
from plenario.models_ import ETLTask


def fetch_pending_tables(model):
    """Used in views.py, fetch all records corresponding to tables pending
    administrator approval. These tables exist in the master tables, but their
    corresponding records have not been ingested.

    :param model: (class) ORM Class corresponding to a meta table
    :returns: (list) contains all records for which is_approved is false"""

    query = session.query(model).filter(model.approved_status is not True)
    return query.all()


def fetch_table_etl_status(model):
    """Used in views.py, fetch all records corresponding to tables that have
    entered the ETL process. Used to report successful, ongoing, or failed
    ETL tasks.

    :param model: (class) ORM Class corresponding to a meta table
    :returns: (list) contains all records for datasets and ETL status"""

    query = session.query(model, ETLTask.status, ETLTask.error)
    return query.all()


def fetch_table(model, dataset_name):

    return model.get_by_dataset_name(dataset_name).point_table


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
