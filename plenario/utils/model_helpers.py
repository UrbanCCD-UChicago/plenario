"""model_helpers: Just a collection of functions which perform common
interactions with the models."""

from plenario.database import session
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

    query = session.query(model, ETLTask.task_status, ETLTask.task_error)
    return query.all()
