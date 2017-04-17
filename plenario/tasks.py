import csv
import logging
import os
import tarfile
from datetime import datetime, timedelta

import boto3
from celery import Celery
from dateutil.parser import parse as date_parse
from raven import Client
from sqlalchemy import Table

from plenario.database import redshift_base, redshift_session
from plenario.database import postgres_session, postgres_base, postgres_engine as engine
from plenario.etl.point import PlenarioETL
from plenario.etl.shape import ShapeETL
from plenario.models import MetaTable, ShapeMetadata
from plenario.settings import CELERY_BROKER_URL, S3_BUCKET
from plenario.settings import PLENARIO_SENTRY_URL, CELERY_RESULT_BACKEND
from plenario.utils.helpers import reflect
from plenario.utils.weather import WeatherETL

client = Client(PLENARIO_SENTRY_URL) if PLENARIO_SENTRY_URL else None

worker = Celery(
    "worker",
    broker=CELERY_BROKER_URL,
    backend=CELERY_RESULT_BACKEND
)

logger = logging.getLogger(__name__)


def get_meta(name: str):
    """Return meta record given a point table name or a shape table name."""

    query = postgres_session.query(MetaTable).filter(MetaTable.dataset_name == name)
    result = query.first()

    if result is None:
        result = postgres_session.query(ShapeMetadata).filter(
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

    metatable = reflect("meta_master", postgres_base.metadata, engine)
    metatable.delete().where(metatable.c.dataset_name == name).execute()
    reflect(name, postgres_base.metadata, engine).drop()
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

    metashape = reflect("meta_shape", postgres_base.metadata, engine)
    metashape.delete().where(metashape.c.dataset_name == name).execute()
    reflect(name, postgres_base.metadata, engine).drop()
    return True


@worker.task()
def frequency_update(frequency) -> bool:
    """Queue an update task for all the tables whose corresponding meta info
    is part of this frequency group."""

    point_metas = postgres_session.query(MetaTable) \
        .filter(MetaTable.update_freq == frequency) \
        .filter(MetaTable.date_added != None) \
        .all()

    for point in point_metas:
        update_dataset.delay(point.dataset_name)

    shape_metas = postgres_session.query(ShapeMetadata) \
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
def archive(datetime_string: str) -> bool:
    """Store the feature data into tar files organized by node and upload
    those tar files to s3."""

    logger.debug('reflecting tables')
    # Get table objects for all known feature tables in redshift database
    redshift_base.metadata.reflect()
    tables = dict(redshift_base.metadata.tables)
    logger.debug('reflected redshift tables')

    try:
        del tables['unknown_feature']
        del tables['array_of_things_chicago__unknown_feature']
        logger.debug('deleted unknown feature keys')
    except KeyError:
        # The unknown feature table might not exist in test environments
        pass

    # Get the start and end datetime bounds for this month
    start, end = start_and_end_of_the_month(date_parse(datetime_string))
    logger.debug('get the start and end dates for the given month')

    # Break each feature of interest table up into csv files grouped by node
    csv_file_groups = []
    for table in tables.values():
        try:
            table.c.node_id
        except AttributeError:
            # Skip tables which are not feature of interest tables
            continue
        # Save the list of generated file names
        csv_file_groups.append(
            table_to_csvs(table, start, end))
        logger.debug('generated csv files for {}'.format(table))

    # Sort the file names into groups by node
    tar_groups = {}
    for file_group in csv_file_groups:
        for file_path in file_group:
            node_path = file_path.split('.', 1)[0]
            tar_groups.setdefault(node_path, []).append(file_path)
    logger.debug("tar'd all the csvs together by node")

    # Tar and upload each group of files for a single node
    for node_path, tar_group in tar_groups.items():

        tarfile_path = '/tmp/{}.tar.gz'.format(node_path)

        tar = tarfile.open(tarfile_path, mode='w:gz')
        for file_path in tar_group:
            tar.add('/tmp/' + file_path)
            os.remove('/tmp/' + file_path)
        tar.close()
        logger.debug("generated {}".format(tarfile_path))

        s3_destination = '{}-{}/{}.tar.gz'.format(start.year, start.month, node_path)
        s3_upload(tarfile_path, s3_destination)
        os.remove(tarfile_path)
        logger.debug("uploaded {}".format(tarfile_path))

    return True


def s3_upload(path: str, dest: str):
    """Upload file found at path to s3."""

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(S3_BUCKET)
    file = open(path, 'rb')

    bucket.put_object(Key=dest, Body=file)
    file.close()


def table_to_csvs(table: Table, start: datetime, end: datetime) -> list:
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
            file = '{}.{}.{}.{}.csv'
            file = file.format(node, table.name, start.date(), end.date())

            file_names.append(file)
            files[node] = open('/tmp/' + file, 'w')
            writers[node] = csv.writer(files[node])
            writers[node].writerow(row.keys())
        writers[node].writerow(row)

    for file in files.values():
        file.seek(0)
        file.close()

    return file_names
