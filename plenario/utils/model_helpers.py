"""model_helpers: Just a collection of functions which perform common
interactions with the models."""

from sqlalchemy.exc import NoSuchTableError
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

    query = session.query(model, ETLTask.status, ETLTask.error)
    return query.all()


def fetch_table(model, dataset_name):

    return model.get_by_dataset_name(dataset_name).point_table


def table_exists(model, dataset_name):
    try:
        fetch_table(model, dataset_name)
        return True
    except (AttributeError, NoSuchTableError):
        return False
