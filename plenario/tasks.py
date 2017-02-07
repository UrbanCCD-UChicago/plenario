import csv
import sqlalchemy.ext.serializer
import tarfile
import tempfile

from celery import Celery
from datetime import datetime, timedelta
from dateutil.parser import parse as date_parse
from raven import Client
from sqlalchemy.orm import sessionmaker

from plenario.database import session as session, Base, app_engine as engine
from plenario.database import redshift_Base as redshift_base
from plenario.database import redshift_session, redshift_engine
from plenario.etl.point import PlenarioETL
from plenario.etl.shape import ShapeETL
from plenario.models import MetaTable, ShapeMetadata
from plenario.settings import PLENARIO_SENTRY_URL, CELERY_RESULT_BACKEND
from plenario.settings import CELERY_BROKER_URL
from plenario.utils.helpers import reflect
from plenario.utils.weather import WeatherETL


client = Client(PLENARIO_SENTRY_URL) if PLENARIO_SENTRY_URL else None

worker = Celery(
    "worker",
    broker=CELERY_BROKER_URL,
    backend=CELERY_RESULT_BACKEND
)


def get_meta(name: str):
    """Return meta record given a point table name or a shape table name."""

    query = session.query(MetaTable).filter(MetaTable.dataset_name == name)
    result = query.first()

    if result is None:
        result = session.query(ShapeMetadata).filter(
            ShapeMetadata.dataset_name == name
        ).first()

    if result is None:
        raise ValueError("dataset '%s' not found in metadata records" % name)

    return result


@worker.task()
def health() -> bool:
    """Shows that the worker is still recieving messages."""

    return True


@worker.task()
def add_dataset(name: str) -> bool:
    """Ingest the row information for an approved point dataset."""

    meta = get_meta(name)
    PlenarioETL(meta).add()
    return True


@worker.task()
def update_dataset(name: str) -> bool:
    """Update the row information for an approved point dataset."""

    meta = get_meta(name)
    PlenarioETL(meta).update()
    return True


@worker.task()
def delete_dataset(name: str) -> bool:
    """Delete the table and meta information for an approved point dataset."""

    metatable = reflect("meta_master", Base.metadata, engine)
    metatable.delete().where(metatable.c.dataset_name == name).execute()
    reflect(name, Base.metadata, engine).drop()
    return True


@worker.task()
def add_shape(name: str) -> bool:
    """Ingest the row information for an approved shapeset."""

    meta = get_meta(name)
    ShapeETL(meta).add()
    return True


@worker.task()
def update_shape(name: str) -> bool:
    """Update the row information for an approved shapeset."""

    meta = get_meta(name)
    ShapeETL(meta).update()
    return True


@worker.task()
def delete_shape(name) -> bool:
    """Delete the table and meta information for an approved shapeset."""

    metashape = reflect("meta_shape", Base.metadata, engine)
    metashape.delete().where(metashape.c.dataset_name == name).execute()
    reflect(name, Base.metadata, engine).drop()
    return True


@worker.task()
def frequency_update(frequency) -> bool:
    """Queue an update task for all the tables whose corresponding meta info
    is part of this frequency group."""

    point_metas = session.query(MetaTable) \
        .filter(MetaTable.update_freq == frequency) \
        .filter(MetaTable.date_added != None) \
        .all()

    for point in point_metas:
        update_dataset.delay(point.dataset_name)

    shape_metas = session.query(ShapeMetadata) \
        .filter(ShapeMetadata.update_freq == frequency) \
        .filter(ShapeMetadata.is_ingested == True) \
        .all()
    
    for shape_meta in shape_metas:
        update_shape.delay(shape_meta.dataset_name)

    return True


@worker.task()
def update_metar() -> True:
    """Run a METAR update."""

    w = WeatherETL()
    w.metar_initialize_current()
    return True


@worker.task()
def clean_metar() -> True:
    """Given the latest datetime available in hourly observations table,
    delete all metar records older than that datetime. Records which exist
    in the hourly table are the quality-controlled versions of records that
    existed in the metar table."""

    WeatherETL().clear_metars()
    return True


@worker.task()
def update_weather(month=None, year=None, wbans=None) -> True:
    """Run a weather update."""

    # This should do the current month AND the previous month, just in case.
    last_month_dt = datetime.now() - timedelta(days=7)
    last_month = last_month_dt.month
    last_year = last_month_dt.year

    if not month:
        month = datetime.now().month
    if not year:
        year = datetime.now().year

    w = WeatherETL()
    if last_month != month:
        w.initialize_month(last_year, last_month, weather_stations_list=wbans)
    w.initialize_month(year, month, weather_stations_list=wbans)
    return True


@worker.task()
def archive(path: str, table: str, start: str, end: str) -> str:
    """Store the results of a query in a tar.gz file."""

    redshift_base.metadata.reflect()

    table = redshift_base.metadata.tables[table]
    query = redshift_session.query(table)  \
        .filter(table.c.datetime >= date_parse(start)) \
        .filter(table.c.datetime <= date_parse(end))
    temp = tempfile.NamedTemporaryFile('wt')
    writer = csv.writer(temp)

    for row in query.yield_per(1000):
        writer.writerow(row)
    temp.file.seek(0)

    tar = tarfile.open(path, mode='w:gz')
    tar.add(temp.name)
    tar.close()
    temp.close()

    return temp.name
