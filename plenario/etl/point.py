from uuid import uuid4

from geoalchemy2.types import Geometry
from sqlalchemy import Table, Column
from sqlalchemy import func, DateTime

from plenario.database import postgres_base
from plenario.database import postgres_engine
from plenario.database import postgres_session
from plenario.etl.etlfile import ETLFileRemote
from plenario.models.meta.schema import infer
from plenario.utils.helpers import slugify


def point(bytestring):
    return b'SRID=4326;POINT (' + bytestring + b')'


def derive_datetime_and_location(line, dt_i, lat_i=-1, lon_i=-1, loc_i=-1):

    if not line:
        return line

    values = line.split(b',')
    datetime = values[dt_i]

    if loc_i >= 0:
        location = point(values[loc_i].strip())
    else:
        latitude = values[lat_i].strip()
        longitude = values[lon_i].strip()
        location = point(b' '.join((latitude, longitude)))

    line = b','.join([line.strip(), datetime, location]) + b'\n'
    return line


def update_meta(metatable, table):

    metatable.update_date_added()

    metatable.obs_from, metatable.obs_to = postgres_session.query(
        func.min(table.c.point_date),
        func.max(table.c.point_date)
    ).first()

    metatable.bbox = postgres_session.query(
        func.ST_SetSRID(
            func.ST_Envelope(func.ST_Union(table.c.geom)),
            4326
        )
    ).first()[0]

    metatable.column_names = {
        c.name: str(c.type) for c in metatable.column_info()
        if c.name not in {'geom', 'point_date', 'hash'}
        }

    postgres_session.add(metatable)
    postgres_session.commit()


def ingest(metadata):

    source = metadata.source_url
    staging_name = str(uuid4())

    geospatial_columns = [
        # Column('id', Integer, primary_key=True),
        Column('datetime', DateTime),
        Column('location', Geometry(geometry_type='POINT', srid=4326))
    ]

    columns = infer(source) + geospatial_columns
    column_names = [slugify(column.name) for column in columns]
    quoted_column_names = ['"%s"' % column_name for column_name in column_names]

    latitude = metadata.latitude
    longitude = metadata.longitude
    location = metadata.location

    datetime_index = column_names.index(metadata.observed_date)
    latitude_index = column_names.index(latitude) if latitude else -1
    longitude_index = column_names.index(longitude) if longitude else -1
    location_index = column_names.index(location) if location else -1

    etlfile = ETLFileRemote(source)
    etlfile.readline()
    etlfile.hook(
        lambda line: derive_datetime_and_location(
            line=line,
            dt_i=datetime_index,
            lat_i=latitude_index,
            lon_i=longitude_index,
            loc_i=location_index
        )
    )

    table = Table(staging_name, postgres_base.metadata, *columns)
    table.create()

    connection = postgres_engine.raw_connection()

    with connection.cursor() as cursor:
        try:

            copy_statement = 'copy "{table}" ({columns}) from stdin '
            copy_statement += "with (delimiter ',', format csv, header true)"
            copy_statement = copy_statement.format(
                table=staging_name,
                columns=','.join(quoted_column_names)
            )
            cursor.copy_expert(copy_statement, etlfile)
            connection.commit()

            index_statement = 'alter table "{}" add column id serial primary key'
            index_statement = index_statement.format(staging_name)

            drop_statement = 'drop table if exists "{}"'.format(metadata.dataset_name)

            rename_statement = 'alter table "{staging}" rename to "{finished}"'
            rename_statement = rename_statement.format(
                staging=staging_name,
                finished=metadata.dataset_name
            )

            for statement in [index_statement, drop_statement, rename_statement]:
                print(statement)
                cursor.execute(statement)
                connection.commit()

        except:
            raise

        finally:
            connection.close()
