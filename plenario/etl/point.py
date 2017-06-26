from sqlalchemy import Table, MetaData
from sqlalchemy import func

from plenario.database import postgres_engine
from plenario.database import postgres_session
from plenario.etl.etlfile import ETLFileRemote
from plenario.models.meta.schema import infer
from plenario.utils.helpers import slugify


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
    staging_name = 'staging_' + metadata.dataset_name
    final_name = metadata.dataset_name

    staging_columns = infer(source)
    staging_column_names = [slugify(c.name) for c in staging_columns]
    quoted_staging_column_names = ['"%s"' % c for c in staging_column_names]

    staging_table = Table(staging_name, MetaData(), *staging_columns)
    staging_table.create(bind=postgres_engine)

    etlfile = ETLFileRemote(source)

    connection = postgres_engine.raw_connection()

    # TODO(heyzoos)
    # This is a gross looking block of code. Let's clean this up
    # when we get the chance.
    with connection.cursor() as cursor:
        try:
            copy_statement = 'copy "{table}" ({columns}) from stdin '
            copy_statement += "with (delimiter ',', format csv, header true)"
            copy_statement = copy_statement.format(
                table=staging_name,
                columns=','.join(quoted_staging_column_names)
            )
            cursor.copy_expert(copy_statement, etlfile)
            connection.commit()

            drop_statement = 'drop table if exists "{}"'.format(metadata.dataset_name)

            rename_statement = 'alter table "{staging}" rename to "{finished}"'
            rename_statement = rename_statement.format(
                staging=staging_name,
                finished=metadata.dataset_name
            )

            alter_statements = [
                'alter table "%s" add column id serial primary key' % final_name,
                'alter table "%s" add column geom geometry(point, 4326)' % final_name,
                'alter table "%s" add column point_date timestamp ' % final_name
            ]

            update_statement =                       \
                'update "%s" as t '                  \
                'set (geom, point_date) = '          \
                '(point_from_loc(t."%s"), t."%s"::timestamp)'

            update_statement %= (metadata.dataset_name, metadata.location, metadata.observed_date)

            statements = [
                drop_statement,
                rename_statement
            ] + alter_statements + [
                update_statement
            ]

            for statement in statements:
                print(statement)
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
