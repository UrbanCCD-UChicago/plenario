from collections import defaultdict
from json import loads

import boto3
import csv
import os
import tarfile

from celery import Celery
from datetime import datetime, timedelta
from dateutil.parser import parse as date_parse
from raven import Client
from sqlalchemy import Table, func, cast, DateTime, alias, types, select, and_, case

from plenario.database import session as session, Base, app_engine as engine
from plenario.database import redshift_base, redshift_session, redshift_engine
from plenario.database import redshift_session_context
from plenario.etl.point import PlenarioETL
from plenario.etl.shape import ShapeETL
from plenario.models import MetaTable, ShapeMetadata
from plenario.models.SensorNetwork import SensorMeta, FeatureMeta
from plenario.settings import PLENARIO_SENTRY_URL, CELERY_RESULT_BACKEND
from plenario.settings import CELERY_BROKER_URL, S3_BUCKET
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
def archive(dt: str) -> bool:
    """Store the feature data into tar files organized by node and upload
    those tar files to s3."""

    # Get table objects for all known feature tables in redshift database
    redshift_base.metadata.reflect()
    tables = dict(redshift_base.metadata.tables)

    try:
        del tables['unknown_feature']
        del tables['array_of_things_chicago__unknown_feature']
    except KeyError:
        # The unknown feature table might not exist in test environments
        pass

    # Get the start and end datetime bounds for this month
    start, end = start_and_end_of_the_month(date_parse(dt))

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

    # Sort the file names into groups by node
    tar_groups = {}
    for file_group in csv_file_groups:
        for file_path in file_group:
            node = file_path.split('--', 1)[0]
            tar_groups.setdefault(node, []).append(file_path)

    # Tar and upload each group of files for a single node
    for node, tar_group in tar_groups.items():

        tarfile_path = '{}.tar.gz'.format(node)

        tar = tarfile.open(tarfile_path, mode='w:gz')
        for file_path in tar_group:
            tar.add(file_path)
            os.remove(file_path)
        tar.close()

        s3_destination = '{}-{}/{}.tar.gz'.format(start.year, start.month, node)
        s3_upload(tarfile_path, s3_destination)
        os.remove(tarfile_path)

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


@worker.task()
def resolve():
    """Run resolve sensor for every distinct sensor available in the unknown
    feature table in redshift."""

    with redshift_session_context() as session:
        rp = session.execute('select distinct sensor from unknown_feature')
        for row in rp:
            try:
                resolved_count = resolve_sensor(row.sensor)
            except AttributeError as err:
                print(err)


@worker.task()
def resolve_sensor(sensor: str):
    """Move values from the staging observation table to the mapped tables. It
    fails on sensors for which the metadata does not exist. If the mapping is
    incorrect the rows will not be moved over at all."""

    sensor = SensorMeta.query.get(sensor)

    for i, feature in enumerate(sensor.features()):
        feature = FeatureMeta.query.get(feature)
        selections = []
        conditions = []
        values = ['node_id, datetime, meta_id, sensor']

        for property_, type_ in feature.types().items():
            # Using 'case when' allows us to resolve to null values if a feature
            # can't be extracted from the data column. If the value is not null,
            # then attempt to cast it to the correct type. 
            selection = "case when json_extract_path_text(data, '{0}') = '' then null "
            selection += "else json_extract_path_text(data, '{0}')::{1} end as {0}"
            selection = selection.format(property_, type_)
            selections.append(selection)

            condition = "json_extract_path_text(data, '{}') != ''".format(property_)
            conditions.append(condition)

            values.append(property_)
        
        # This allows us to select only rows where at least one of the properties
        # found in the sensor metadata can be extracted from the raw data string
        # in the unknown feature table.
        conditions = "(" + str.join(" or ", conditions) + ")"
        selections = str.join(", ", selections)
        # Necessary so that the ordering of the selected columns doesn't matter
        values = str.join(", ", values)

        select = 'select node_id, "datetime"::datetime, '
        select += 'meta_id, sensor, {} from unknown_feature'.format(selections)

        delete = 'delete from unknown_feature'

        for network in feature.networks:
            target_table = "{}__{}".format(network.name, feature.name)
            insert = 'insert into {} ({}) '.format(target_table, values)

            where = "where network = '{}' and sensor = '{}' and {}"
            where = where.format(network.name, sensor.name, conditions)

            redshift_session.execute('{} {} {}'.format(insert, select, where))

            # In cases where the sensor reports on two features (ex. bmp180) we
            # can't delete until we've mapped to every feature table.
            if i + 1 == len(sensor.features()):
                redshift_session.execute('{} {}'.format(delete, where))