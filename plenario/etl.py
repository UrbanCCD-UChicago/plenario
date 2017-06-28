from tempfile import TemporaryFile
from zipfile import ZipFile

from requests import get
from sqlalchemy import Table, MetaData
from sqlalchemy import func
from sqlalchemy.exc import ProgrammingError

from plenario.database import postgres_engine, postgres_session
from plenario.etlfile import ETLFileLocal, ETLFileRemote
from plenario.models.meta.schema import infer_remote
from plenario.utils.helpers import slugify
from plenario.utils.shapefile import import_shapefile


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
        if c.name not in {'geom', 'point_date', 'hash', 'id'}
    }

    postgres_session.add(metatable)
    postgres_session.commit()


def ingest_points(metadata, local=False):

    latitude = metadata.latitude
    longitude = metadata.longitude
    location = metadata.location

    source = metadata.source_url
    staging_name = 'staging_' + metadata.dataset_name
    final_name = metadata.dataset_name

    staging_columns = infer_remote(source)

    for column in staging_columns:
        if column.name == 'id':
            column.name = '%s_id' % final_name

    staging_column_names = [slugify(c.name) for c in staging_columns]
    quoted_staging_column_names = ['"%s"' % c for c in staging_column_names]

    staging_table = Table(staging_name, MetaData(), *staging_columns)
    staging_table.create(bind=postgres_engine)

    if local:
        etlfile = ETLFileLocal(source)
    else:
        etlfile = ETLFileRemote(source)

    connection = postgres_engine.raw_connection()

    try:

        with connection.cursor() as cursor:
            copy_statement = 'copy "%s" (%s) from stdin '
            copy_statement += "with (delimiter ',', format csv, header true)"
            copy_statement %= staging_name, ','.join(quoted_staging_column_names)

            cursor.copy_expert(copy_statement, etlfile)
            connection.commit()

            drop_statement = 'drop table if exists "%s"' % final_name

            rename_statement = 'alter table "%s" rename to "%s"'
            rename_statement %= staging_name, final_name

            alter_statements = [
                'alter table "%s" add column id serial primary key' % final_name,
                'alter table "%s" add column geom geometry(point, 4326)' % final_name,
                'alter table "%s" add column point_date timestamp ' % final_name
            ]

            update_statement = 'update "%s" as t set (geom, point_date) = '
            if location:
                update_statement += '(point_from_loc(t."%s"), t."%s"::timestamp)'
                update_statement %= (metadata.dataset_name, metadata.location, metadata.observed_date)
            else:
                update_statement += '(st_setsrid(st_point(t."%s", t."%s"), 4326), t."%s"::timestamp)'
                update_statement %= (metadata.dataset_name, longitude, latitude, metadata.observed_date)

            statements = [drop_statement, rename_statement]
            statements += alter_statements
            statements.append(update_statement)

            for statement in statements:
                cursor.execute(statement)
                connection.commit()

    except:
        raise

    finally:
        connection.close()
        etlfile.close()

    final_table = Table(
        metadata.dataset_name,
        MetaData(),
        extend_existing=True,
        autoload_with=postgres_engine
    )

    update_meta(metadata, final_table)

    return final_table


def ingest_shapes(metashape, local=False):

    source = metashape.source_url
    staging_name = 'staging_' + metashape.dataset_name
    final_name = metashape.dataset_name

    shapefile = TemporaryFile()

    if local:
        shapefile = open(source, 'rb')
    else:
        stream = get(source, stream=True).iter_content(8192)
        for chunk in stream:
            if chunk:
                shapefile.write(chunk)
        stream.close()

    with ZipFile(shapefile) as shapezip:
        import_shapefile(shapezip, staging_name)

        try:
            postgres_engine.execute('drop table {}'.format(final_name))
        except ProgrammingError:
            pass

        rename_table = 'alter table {} rename to {}'
        rename_table = rename_table.format(staging_name, final_name)
        postgres_engine.execute(rename_table)

        metashape.update_after_ingest()
        postgres_session.commit()
