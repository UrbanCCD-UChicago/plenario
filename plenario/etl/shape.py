# -*- coding: utf-8 -*-

import zipfile

from plenario.database import session, app_engine as engine
from plenario.etl.common import ETLFile, PlenarioETLError, add_unique_hash,\
    delete_absent_hashes
from plenario.utils.shapefile import import_shapefile, ShapefileError
from sqlalchemy import Table, MetaData


def reflect(table_name):
    """
    Given the name of a table present in the database,
    return a SQLAlchemy Table object that represents it.
    :param table_name:
    """
    return Table(table_name, MetaData(),
                 autoload_with=engine, extend_existing=True)


class ShapeETL:

    def __init__(self, meta, source_path=None):
        self.source_path = source_path
        self.table_name = meta.dataset_name
        self.source_url = meta.source_url
        self.meta = meta

    def add(self):
        if self.meta.is_ingested:
            raise PlenarioETLError("Table {} has already been ingested.".
                                   format(self.table_name))
        
        new = HashedShape(self.table_name, self.source_url, self.source_path)
        try:
            new.ingest()
            self.meta.update_after_ingest()
            session.commit()
        except:
            # In case ingestion failed partway through,
            # be sure to leave no trace.
            new.drop()
            raise

    def update(self):
        assert self.meta.is_ingested
        existing = reflect(self.table_name)
        staging_name = 's_' + self.table_name

        with HashedShape(staging_name, self.source_url,
                         self.source_path) as staging:
            self._hash_update(staging, existing)

        self.meta.update_after_ingest()

    @staticmethod
    def _hash_update(staging, existing):
        delete_absent_hashes(staging.name, existing.name)

        # Select all records from staging
        # whose hashes are not present in existing.
        join_condition = staging.c['hash'] == existing.c['hash']
        sel = staging.select()\
            .select_from(staging.outerjoin(existing, join_condition)).\
            where(existing.c['hash'] == None)

        # Insert those into existing
        col_names = [col.name for col in existing.columns]
        ins = existing.insert().from_select(col_names, sel)
        try:
            engine.execute(ins)
        except Exception as e:
            raise PlenarioETLError(repr(e) + '\n' + str(sel))


class HashedShape(object):
    """
    Ingest a shapefile,
    and append an md5 hash column.
    """

    def __init__(self, name, url, path=None):
        self.name = name
        self.url = url
        self.path = path

    def ingest(self):
        """
        Create the table. The caller is responsible for cleanup.
        :return: SQLAlchemy Table
        """

        with ETLFile(source_url=self.url, source_path=self.path, interpret_as='bytes') as file_helper:

            # Attempt insertion
            try:
                with zipfile.ZipFile(file_helper.handle) as shapefile_zip:
                    import_shapefile(shapefile_zip=shapefile_zip,
                                     table_name=self.name)
            except zipfile.BadZipfile:
                raise PlenarioETLError("Source file was not a valid .zip")
            except ShapefileError as e:
                raise PlenarioETLError("Failed to import shapefile.\n{}".
                                       format(repr(e)))

        add_unique_hash(self.name)
        return reflect(self.name)

    def __enter__(self):
        """
        If user of this class does not want to be responsible for cleanup,
        use the class as a context manager.
        """
        return self.ingest()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.drop()

    def drop(self):
        engine.execute("DROP TABLE IF EXISTS {};".format(self.name))