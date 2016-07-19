from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy import select, update, func
from sqlalchemy.exc import IntegrityError
from plenario.database import app_engine, Base, session
from plenario.models import MetaTable


# Notes
# -----
# 1) Is there any point in offering a choice for add_task? No matter what,
#    it means that a dataset has been newly approved and will be pending
#    ingestion. There will never be a situation where you add a completed
#    task. A change here will cause test_fetch_etl_status test to break.


# These dictionaries help to keep me from making errors. If a message or
# status is misspelled, it will cause a fast and loud KeyError, saving me
# the trouble of weeding out the issue somewhere down the line.

ETLStatus = {
    'pending': 'Ingest Pending',
    'started': 'Ingest Started',
    'success': 'SUCCESS',
    'failure': 'Failure'
}

ETLType = {
    'dataset': 'master',
    'shapeset': 'shape'
}


class ETLTask(Base):
    """Store information about completed jobs pertaining to ETL actions."""

    __tablename__ = 'etl_task'
    task_id = Column(Integer, primary_key=True)
    dataset_name = Column(String, nullable=False, unique=True)
    date_done = Column(DateTime)
    status = Column(String)
    error = Column(String)
    type = Column(String)


def add_task(dataset_name, status, error, type_):
    """Used primarily in the views, called whenever a dataset is added
    by an administrator or a dataset is approved. Used to create a new
    ETLTask record.

    :param dataset_name: (string)
    :param status: (string) best to use a status from the ETLStatus dict
    :param error: (string) printout of an exception and traceback
    :param type_: (string) differentiates between tables, best to use a type
                           from the ETLType dict"""

    task = ETLTask(dataset_name=dataset_name,
                   status=status,
                   error=error,
                   type=type_)

    try:
        session.add(task)
        session.commit()
    except IntegrityError:
        session.rollback()


def update_task(dataset_name, date_done, status, error):
    """Used when a dataset completes or fails. Updates a single ETLTask.

    :param dataset_name: (string)
    :param date_done: (DateTime) marks when the task was completed by a worker
    :param status: (string) best to use a status from the ETLStatus dict
    :param error: (string) printout of an exception and traceback"""

    session.execute(
        update(
            ETLTask,
            values={
                ETLTask.status: status,
                ETLTask.error: error,
                ETLTask.date_done: date_done
            }
        ).where(ETLTask.dataset_name == dataset_name)
    )

    try:
        session.commit()
    # TODO: Figure out what this breaks on exactly.
    except:
        session.rollback()


def fetch_task(dataset_name):
    """Generally used for testing, selects a single ETLTask corresponding
    to some dataset.

    :param dataset_name: (string)"""

    q = "select * from etl_task where dataset_name = '{}'".format(dataset_name)
    return app_engine.execute(q).first()


def delete_task(dataset_name):
    """Also generally used for testing, deletes a ETLTask corresponding to some
    dataset.

    :param dataset_name: (string) a table name"""

    q = "delete from etl_task where dataset_name = '{}'".format(dataset_name)
    app_engine.execute(q)


def fetch_pending_tables(model):
    """Used in views.py, fetch all records corresponding to tables pending
    administrator approval. These tables exist in the master tables, but their
    corresponding records have not been ingested.

    :param model: (class) ORM Class corresponding to a meta table
    :returns: (list) contains all records for which is_approved is false"""

    query = session.query(model).filter(model.approved_status == 'f')
    return query.all()


def fetch_table_etl_status(type_):
    """Used in views.py, fetch all records corresponding to tables that have
    entered the ETL process. Used to report successful, ongoing, or failed
    ETL tasks.

    :param type_: (string) designates what tasks to return
    :returns: (list) contains all records for datasets and ETL status"""

    q = "select * from meta_{}, etl_task where type = '{}'".format(type_, type_)
    return app_engine.execute(q).fetchall()


def dataset_status_info_query(source_url_hash=None):
    # TODO: Make better.

    columns = "meta.human_name, meta.source_url_hash, etl.error, etl.status, etl.date_done, etl.task_id"

    q = '''SELECT {}
        FROM meta_master AS meta, etl_task AS etl
        WHERE etl.date_done IS NOT NULL
    '''.format(columns)

    q += "AND m.source_url_hash = :source_url_hash" if source_url_hash else ""
    q += " ORDER BY etl.task_id DESC LIMIT 1000"

    return q


if __name__ == '__main__':
    Base.metadata.create_all()
