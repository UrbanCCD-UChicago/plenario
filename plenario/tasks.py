from celery import Celery
from datetime import datetime, timedelta
from raven import Client

from plenario.api.jobs import submit_job
from plenario.database import session as session, Base, app_engine as engine
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
def update_weather() -> True:
    """Run a weather update."""

    # This should do the current month AND the previous month, just in case.
    last_month_dt = datetime.now() - timedelta(days=7)
    last_month = last_month_dt.month
    last_year = last_month_dt.year

    month, year = datetime.now().month, datetime.now().year
    w = WeatherETL()
    if last_month != month:
        w.initialize_month(last_year, last_month)
    w.initialize_month(year, month)
    return True
