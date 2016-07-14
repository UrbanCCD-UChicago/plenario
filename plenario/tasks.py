from datetime import datetime, timedelta

from raven.conf import setup_logging
from raven.handlers.logging import SentryHandler
from sqlalchemy.exc import NoSuchTableError, InternalError

from plenario.database import session as session, app_engine as engine
from plenario.etl.shape import ShapeETL
from plenario.models import MetaTable, ShapeMetadata
from plenario.settings import CELERY_SENTRY_URL
from plenario.etl.point import PlenarioETL
from plenario.utils.weather import WeatherETL

if CELERY_SENTRY_URL:
    handler = SentryHandler(CELERY_SENTRY_URL)
    setup_logging(handler)


def task_complete_msg(task_name, mt):
    """Create a confirmation message for a completed task.

    :param task_name: (string) task name, like 'update' or 'delete'
    :param mt: (MetaTable Record) meta info about the target dataset
    :returns: (string) confirmation message"""

    # Check for attributes which differenciate MetaTable from ShapeTable.
    tname = mt.human_name if hasattr(mt, 'human_name') else mt.dataset_name
    source = mt.source_url if hasattr(mt, 'source_url') else mt.source_url_hash
    return "Finished {} for {} ({})".format(task_name, tname, source)


def add_dataset(source_url_hash):
    """Ingest the row information for an approved dataset.

    :param source_url_hash: (string) identifier used to grab target table info
    :returns: (string) a helpful confirmation message"""

    metatable = session.query(MetaTable).get(source_url_hash)
    PlenarioETL(metatable).add()
    return task_complete_msg('ingest', metatable)


def update_dataset(source_url_hash):
    """Update the row information for an approved dataset.

    :param source_url_hash: (string) identifier used to grab target table info
    :returns: (string) a helpful confirmation message"""

    metatable = session.query(MetaTable).get(source_url_hash)
    PlenarioETL(metatable).update()
    return task_complete_msg('update', metatable)


def delete_dataset(source_url_hash):
    """Delete the row information and meta table for an approved dataset.

    :param source_url_hash: (string) identifier used to grab target table
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
    except InternalError, e:
        raise delete_dataset.retry(exc=e)
    return task_complete_msg('deletion', meta)


def add_shape(table_name):
    """Ingest the row information for an approved shapeset.

    :param table_name: (string) identifier used to grab target table info
    :returns: (string) a helpful confirmation message"""

    meta = session.query(ShapeMetadata).get(table_name)
    ShapeETL(meta).add()
    return task_complete_msg('ingest', meta)


def update_shape(table_name):
    """Update the row information for an approved shapeset.

    :param table_name: (string) identifier used to grab target table info
    :returns: (string) a helpful confirmation message"""

    meta = session.query(ShapeMetadata).get(table_name)
    ShapeETL(meta=meta).update()
    return task_complete_msg('update', meta)


def delete_shape(table_name):
    """Delete the row and meta information for an approved shapeset.

    :param table_name: (string) identifier used to grab target table info
    :returns: (string) a helpful confirmation message"""

    shape_meta = session.query(ShapeMetadata).get(table_name)
    shape_meta.remove_table()
    session.commit()
    return task_complete_msg('deletion', shape_meta)


def frequency_update(frequency):
    # hourly, daily, weekly, monthly, yearly
    md = session.query(MetaTable)\
        .filter(MetaTable.update_freq == frequency)\
        .filter(MetaTable.date_added != None)\
        .all()
    for m in md:
        update_dataset.delay(m.source_url_hash)

    md = session.query(ShapeMetadata)\
        .filter(ShapeMetadata.update_freq == frequency)\
        .filter(ShapeMetadata.is_ingested == True)\
        .all()
    for m in md:
        update_shape.delay(m.dataset_name)
    return '%s update complete' % frequency


def update_metar():
    print "update_metar()"
    w = WeatherETL()
    w.metar_initialize_current()
    return 'Added current metars'


def update_weather():
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
