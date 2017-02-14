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
from sqlalchemy import Table, func

from plenario.database import session as session, Base, app_engine as engine
from plenario.database import redshift_base, redshift_session, redshift_engine
from plenario.etl.point import PlenarioETL
from plenario.etl.shape import ShapeETL
from plenario.models import MetaTable, ShapeMetadata
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


def map_unknown_to_foi(unknown, sensor_properties):
    """Given a valid unknown feature row, distribute the data stored within
    to the corresponding feature of interest tables by using the mapping
    defined by a sensor's observed properties.

    :param unknown: (object) a row returned from a SQLAlchemy query
    :param sensor_properties: (dict) holds mappings from node key to FOI"""

    # TODO: Make sure to handle errors, in case the resolved issue doesn't
    # TODO: actually fix what made this these observations misfits.

    foi_insert_vals = defaultdict(list)

    for key, value in list(loads(unknown.data).items()):
        foi = sensor_properties[key].split(".")[0]
        prop = sensor_properties[key].split(".")[1]
        foi_insert_vals[foi].append((prop, value))

    for foi, insert_vals in list(foi_insert_vals.items()):
        insert = "insert into {} (node_id, datetime, meta_id, sensor, {}) values ({})"
        columns = ", ".join(val[0] for val in insert_vals)

        values = "'{}', '{}', '{}', '{}', ".format(
            unknown.node_id,
            unknown.datetime,
            unknown.meta_id,
            unknown.sensor
        ) + ", ".join(repr(val[1]) for val in insert_vals)

        redshift_engine.execute(insert.format(foi, columns, values))

        delete = "delete from unknown_feature where node_id = '{}' and datetime = '{}' and meta_id = '{}' and sensor = '{}'"
        delete = delete.format(unknown.node_id, unknown.datetime, unknown.meta_id, unknown.sensor)

        redshift_engine.execute(delete)


@worker.task()
def unknown_features_resolve(target_sensor) -> int:
    """When the issues for a sensor with an unknown error have been resolved,
    attempt to recover sensor readings that were originally suspended in the
    unknowns table and insert them into the correct feature of interest table.

    :param target_sensor: (str) resolved sensor"""

    print("Resolving: {}".format(target_sensor))

    sensors = reflect("sensor__sensors", Base.metadata, engine)
    unknowns = reflect("unknown_feature", redshift_session.metadata, redshift_engine)

    # Grab the set of keys that are used to assert if an unknown is correct
    c_obs_props = sensors.c.observed_properties
    q = session.query(c_obs_props).filter(sensors.c.name == target_sensor)
    sensor_properties = q.first()[0]

    print("Most up to date map: {}".format(sensor_properties))

    # Grab all the candidate unknown observations
    c_sensor = unknowns.c.sensor
    target_unknowns = redshift_session \
        .query(unknowns) \
        .filter(c_sensor == target_sensor)

    unresolved_count = redshift_session \
        .query(func.count(unknowns.c.datetime)) \
        .filter(c_sensor == target_sensor) \
        .scalar()

    print("Attempting to resolve {} rows".format(unresolved_count))

    resolved = 0
    for unknown in target_unknowns:

        unknown_data = loads(unknown.data)
        unknown_properties = list(unknown_data.keys())
        known_properties = list(sensor_properties.keys())

        if not all(key in known_properties for key in unknown_properties):
            continue

        map_unknown_to_foi(unknown, sensor_properties)
        resolved += 1

    return resolved