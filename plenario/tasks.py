import boto3
import csv
import os
import tarfile

from celery import Celery
from datetime import datetime, timedelta
from raven import Client
from sqlalchemy import Table

from plenario.database import session as session, Base, app_engine as engine
from plenario.database import redshift_Base as redshift_base
from plenario.database import redshift_session
from plenario.etl.point import PlenarioETL
from plenario.etl.shape import ShapeETL
from plenario.models import MetaTable, ShapeMetadata
from plenario.models.SensorNetwork import NodeMeta
from plenario.settings import PLENARIO_SENTRY_URL, CELERY_RESULT_BACKEND
from plenario.settings import CELERY_BROKER_URL
from plenario.utils.helpers import reflect
from plenario.utils.weather import WeatherETL

from config import Config


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
def update_metar() -> bool:
    """Run a METAR update."""

    w = WeatherETL()
    w.metar_initialize_current()
    return True


@worker.task()
def clean_metar() -> bool:
    """Given the latest datetime available in hourly observations table,
    delete all metar records older than that datetime. Records which exist
    in the hourly table are the quality-controlled versions of records that
    existed in the metar table."""

    WeatherETL().clear_metars()
    return True


@worker.task()
def update_weather(month=None, year=None, wbans=None) -> bool:
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


def start_and_end_of_the_month(dt: datetime):
    """Get first of month and first of next month for a given datetime."""

    start = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if start.month == 12:
        end = start.replace(year=start.year + 1, month=1, day=1, hour=0,
                            minute=0, second=0, microsecond=0)
    else:
        end = start.replace(month=start.month + 1)
    return start, end


@worker.task()
def archive() -> bool:

    # Get table objects for all known feature tables in redshift database
    tables = dict(redshift_base.metadata.tables)
    del tables['array_of_things_chicago__unknown_feature']

    # Get the start and end datetime bounds for this month
    start, end = start_and_end_of_the_month(datetime.now())

    # Break each feature of interest table up into csv files grouped by node
    csv_file_groups = []
    for table in tables.values():
        # Save the list of generated file names
        csv_file_groups.append(csvify(table, start, end))

    # Get the node ids for the current network
    network = 'array_of_things_chicago'
    nodes = NodeMeta.index(network)

    # Reserve list for each node that will contain a nodes relevant file names
    tar_groups = {}
    for node in nodes:
        tar_groups[node] = []

    # Sort the file names into groups by node
    for file_group in csv_file_groups:
        for file_name in file_group:
            node = file_name.split('--')[0]
            tar_groups[node].append(file_name)

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(Config.S3_BUCKET)

    # Tar and upload each group of files for a single node
    for node, tar_group in tar_groups.items():
        tar = tarfile.open('{}.tar.gz'.format(node), mode='w:gz')
        for file_name in tar_group:
            print(file_name)
            tar.add(file_name)
            os.remove(file_name)
        tar.close()

        file = open('{}.tar.gz'.format(node), 'rb')
        bucket.put_object(
            Key='{}-{}/{}.tar.gz'.format(start.year, start.month, node),
            Body=file)
        file.close()
        os.remove('{}.tar.gz'.format(node))

    return True


def csvify(table: Table, start: datetime, end: datetime) -> list:
    """Take a feature of interest table and split it into a bunch of csv files
    that are grouped by node. Return the names of all the files it made."""

    files = {}
    writers = {}
    file_names = []

    query = redshift_session.query(table)  \
        .filter(table.c.datetime >= start) \
        .filter(table.c.datetime < end)    \
        .yield_per(1000)

    for row in query:
        node = row.node_id
        if node not in files:
            file_name = '{}--{}--{}--{}.csv'.format(
                node, table.name, start.date(), end.date())
            file_names.append(file_name)
            files[node] = open(file_name, 'w')
            writers[node] = csv.writer(files[node])
            writers[node].writerow(row.keys())
        writers[node].writerow(row)

    for file in files.values():
        file.seek(0)
        file.close()

    return file_names
