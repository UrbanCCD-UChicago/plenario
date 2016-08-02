import traceback

from datetime import datetime, timedelta
from functools import wraps
from raven.conf import setup_logging
from raven.handlers.logging import SentryHandler
from sqlalchemy.exc import NoSuchTableError, InternalError

from plenario.database import session as session, app_engine as engine
from plenario.etl.point import PlenarioETL
from plenario.etl.shape import ShapeETL
from plenario.api.jobs import submit_job
from plenario.models import MetaTable, ShapeMetadata
from plenario.models_.ETLTask import update_task, ETLStatus, delete_task
from plenario.settings import CELERY_SENTRY_URL
from plenario.utils.weather import WeatherETL

if CELERY_SENTRY_URL:
    handler = SentryHandler(CELERY_SENTRY_URL)
    setup_logging(handler)


def task_complete_msg(task_name, mt):
    """Create a confirmation message for a completed task.

    :param task_name: (string) task name, like 'update' or 'delete'
    :param mt: (MetaTable Record) meta info about the target dataset
    :returns: (string) confirmation message"""

    # Check for attributes which differentiate MetaTable from ShapeTable.
    tname = mt.human_name if hasattr(mt, 'human_name') else mt.dataset_name
    source = mt.source_url if hasattr(mt, 'source_url') else mt.source_url_hash
    return "Finished {} for {} ({})".format(task_name, tname, source)


def etl_report(fn):
    """Decorator for ETL task methods. Takes care of recording the status of
    each task to PostgreSQL. Also reports failed tasks to Sentry."""

    @wraps(fn)
    def wrapper(identifier):

        meta = session.query(MetaTable).get(identifier)
        if meta is None:
            meta = session.query(ShapeMetadata).get(identifier)

        try:
            try:
                # This method updates the last_update attribute, and does
                # not modify date_added unless it does not exist.
                meta.update_date_added()
            except AttributeError:
                # ShapeMetadata has no last_update attribute.
                pass
            update_task(meta.dataset_name, None, ETLStatus['started'], None)
            completion_msg = fn(meta)
            update_task(meta.dataset_name, datetime.now(), ETLStatus['success'], None)
            return completion_msg
        except Exception:
            update_task(meta.dataset_name, datetime.now(), ETLStatus['failure'], traceback.format_exc())
            # TODO: Report to Sentry.

    return wrapper


@etl_report
def add_dataset(meta):
    """Ingest the row information for an approved dataset.

    :param meta: (MetaTable record) identifier used to grab target table info
    :returns: (string) a helpful confirmation message"""

    PlenarioETL(meta).add()
    return task_complete_msg('ingest', meta)


@etl_report
def update_dataset(meta):
    """Update the row information for an approved dataset.

    :param meta: (MetaTable record) identifier used to grab target table info
    :returns: (string) a helpful confirmation message"""

    PlenarioETL(meta).update()
    return task_complete_msg('update', meta)


def delete_dataset(source_url_hash):
    """Delete the row information and meta table for an approved dataset.

    :param source_url_hash: (string) identifier used to grab target table info
    :returns: (string) a helpful confirmation message"""

    meta = session.query(MetaTable).get(source_url_hash)
    try:
        dat_table = meta.point_table
        dat_table.drop(engine, checkfirst=True)
    except NoSuchTableError:
        # Move on so we can get rid of the metadata
        pass
    session.delete(meta)

    try:
        session.commit()
        delete_task(meta.dataset_name)
    except InternalError:
        session.rollback()

    return task_complete_msg('deletion', meta)


@etl_report
def add_shape(meta):
    """Ingest the row information for an approved shapeset.

    :param meta: (MetaTable record) identifier used to grab target table info
    :returns: (string) a helpful confirmation message"""

    ShapeETL(meta).add()
    return task_complete_msg('ingest', meta)


@etl_report
def update_shape(meta):
    """Update the row information for an approved shapeset.

    :param meta: (MetaTable record) identifier used to grab target table info
    :returns: (string) a helpful confirmation message"""

    ShapeETL(meta).update()
    return task_complete_msg('update', meta)


def delete_shape(table_name):
    """Delete the row and meta information for an approved shapeset.

    :param table_name: (string) identifier used to grab target table info
    :returns: (string) a helpful confirmation message"""

    shape_meta = session.query(ShapeMetadata).get(table_name)
    shape_meta.remove_table()
    delete_task(shape_meta.dataset_name)
    session.commit()
    return task_complete_msg('deletion', shape_meta)


def frequency_update(frequency):
    """Queue an update task for all the tables whose corresponding meta info
    is part of this frequency group.

    :param frequency: (string) how often these tables are meant to be updated,
                               can be: often, daily, weekly, monthly, yearly
    :returns (string) confirmation message"""

    md = session.query(MetaTable) \
        .filter(MetaTable.update_freq == frequency) \
        .filter(MetaTable.date_added != None) \
        .all()
    for m in md:
        print "submitted job"
        submit_job({"endpoint": "update_dataset", "query": m.source_url_hash})

    md = session.query(ShapeMetadata) \
        .filter(ShapeMetadata.update_freq == frequency) \
        .filter(ShapeMetadata.is_ingested == True) \
        .all()
    for m in md:
        print "submitted job"
        submit_job({"endpoint": "update_shape", "query": m.dataset_name})

    return '%s updates queued.' % frequency


def update_metar():
    """Run a METAR update.

    :returns (string) confirmation message"""

    w = WeatherETL()
    w.metar_initialize_current()
    return 'Added current metars'


def update_weather():
    """Run a weather update.

    :returns (string) confirmation message"""

    # This should do the current month AND the previous month, just in case.
    lastMonth_dt = datetime.now() - timedelta(days=1)
    lastMonth = lastMonth_dt.month
    lastYear = lastMonth_dt.year

    month, year = datetime.now().month, datetime.now().year
    w = WeatherETL()
    if lastMonth != month:
        w.initialize_month(lastYear, lastMonth)
    w.initialize_month(year, month)

    # Given that this was the most recent month, year, call this function,
    # which will figure out the most recent hourly weather observation and
    # delete all metars before that datetime.
    w.clear_metars()
    return 'Added weather for %s %s' % (month, year)
