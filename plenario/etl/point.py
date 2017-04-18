# import json

# from csvkit.unicsv import UnicodeCSVReader
import csv
from logging import getLogger
from geoalchemy2 import Geometry
from sqlalchemy import TIMESTAMP, Table, Column, MetaData, String
from sqlalchemy import select, func
from sqlalchemy.exc import NoSuchTableError

from plenario.database import postgres_base, postgres_engine
from plenario.database import postgres_session_context
from plenario.etl.common import ETLFile, add_unique_hash, PlenarioETLError, delete_absent_hashes
from plenario.utils.helpers import iter_column, slugify

logger = getLogger(__name__)


class PlenarioETL(object):
    def __init__(self, metadata, source_path=None):
        """
        :param metadata: MetaTable instance of dataset being ETL'd.
        :param source_path: If provided, get source CSV from local filesystem
                            instead of URL in metadata.
        """

        logger.info('Begin.')
        logger.info('metadata: {}'.format(metadata))
        logger.info('source_path: {}'.format(source_path))
        self.metadata = metadata
        # Grab a record of the names we'll need to work with this dataset
        # instead of passing around the unwieldy metadata object to ETL objects.
        # Type of namedtuple('Dataset', 'name date lat lon loc')
        self.dataset = self.metadata.meta_tuple()
        self.staging_table = Staging(self.metadata, source_path=source_path)
        logger.info('End.')

    def add(self):
        """
        Create point table for the first time.
        """
        logger.info('Begin.')
        with self.staging_table as s_table:
            new_table = Creation(s_table.table, self.dataset).table
        update_meta(self.metadata, new_table)
        logger.info('End.')
        return new_table

    def update(self):
        """
        Insert new records into existing point table.
        """
        logger.info('Begin.')
        existing_table = self.metadata.point_table
        with self.staging_table as s_table:
            staging = s_table.table
            delete_absent_hashes(staging.name, existing_table.name)
            with Update(staging, self.dataset, existing_table) as new_records:
                new_records.insert()
        update_meta(self.metadata, existing_table)
        logger.info('End.')


class Staging(object):
    """
    A temporary table that will contain all records from the source CSV.
    From it, either create a new point table
    or insert records from it into an existing point table.
    """

    def __init__(self, meta, source_path=None):
        """
        :param meta: record from MetaTable
        :param source_path: path of source file on local filesystem
                            if None, look for data at a remote URL instead
        """
        # Just the info about column names we usually need
        logger.info('Begin.')
        logger.info('meta: {}'.format(meta))
        logger.info('source_path: {}'.format(source_path))
        self.dataset = meta.meta_tuple()
        self.name = 's_' + self.dataset.name

        # Get the Columns to construct our table
        try:
            # Can we just grab columns from an existing table?
            self.cols = self._from_ingested(meta.column_info())
        except NoSuchTableError:
            self.cols = None

        # Retrieve the source file
        try:
            if source_path:  # Local ingest
                self.file_helper = ETLFile(source_path=source_path)
            else:  # Remote ingest
                self.file_helper = ETLFile(source_url=meta.source_url)
        except Exception as e:
            raise PlenarioETLError(e)

    def __enter__(self):
        """Create the staging table. Will be named s_[dataset_name]"""

        logger.info('Begin.')
        with self.file_helper as helper:
            text_handle = open(helper.handle.name, "rt")

            if not self.cols:
                # We couldn't get the column metadata from an existing table
                self.cols = self._from_inference(text_handle)

            # Grab the handle to build a table from the CSV
            try:
                self.table = self._make_table(text_handle)
                add_unique_hash(self.table.name)
                self.table = Table(
                    self.name,
                    postgres_base.metadata,
                    autoload_with=postgres_engine,
                    extend_existing=True
                )
                return self
            except Exception as e:
                raise PlenarioETLError(e)
        logger.info('End.')

    def _drop(self):
        postgres_engine.execute("DROP TABLE IF EXISTS {};"
                                .format('s_' + self.dataset.name))

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Drop the staging table if it's been created.
        """
        self._drop()

    def _make_table(self, f):
        """
        Create a table and fill it with CSV data.
        :param f: Open file handle pointing to start of CSV
        :return: populated table
        """
        # Persist an empty table eagerly
        # so that we can access it when we drop down to a raw connection.

        # Be paranoid and remove the table if one by this name already exists.
        table = Table(self.name, MetaData(), *self.cols, extend_existing=True)
        self._drop()
        table.create(bind=postgres_engine)

        # Fill in the columns we expect from the CSV.
        names = ['"' + c.name + '"' for c in self.cols]
        copy_st = "COPY {t_name} ({cols}) FROM STDIN " \
                  "WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')".\
            format(t_name=self.name, cols=', '.join(names))

        # In order to issue a COPY, we need to drop down to the psycopg2 DBAPI.
        conn = postgres_engine.raw_connection()
        try:
            with conn.cursor() as cursor:
                f.seek(0)
                cursor.copy_expert(copy_st, f)
                conn.commit()
                return table
        except Exception as e:
            # When the bulk copy fails on _any_ row,
            # roll back the entire operation.
            raise PlenarioETLError(e)
        finally:
            conn.close()

    '''Utility methods to generate columns
    into which we can dump the CSV data.'''

    @staticmethod
    def _from_ingested(column_info):
        """
        Generate columns from the existing table.
        """

        logger.info('Begin.')
        ingested_cols = column_info
        # Don't include the geom and point_date columns.
        # They're derived from the source data
        # and won't be present in the source CSV
        original_cols = [c for c in ingested_cols if c.name not in ['geom', 'point_date', 'hash']]
        # Make copies that don't refer to the existing table.
        cols = [_copy_col(c) for c in original_cols]

        logger.info('End.')
        return cols

    @staticmethod
    def _from_inference(f):
        """Generate columns by scanning CSV and inferring column types."""

        logger.info('Begin.')
        reader = csv.reader(f)
        header = list(map(slugify, next(reader)))

        cols = []
        for col_idx, col_name in enumerate(header):
            col_type, nullable = iter_column(col_idx, f)
            cols.append(_make_col(col_name, col_type, nullable))

        logger.info('End.')
        return cols


def _null_malformed_geoms(existing):
    # We decide to set the geom to NULL when the given lon/lat is (0,0)
    # (off the coast of Africa).
    upd = existing.update().values(geom=None).\
        where(existing.c.geom == select([func.ST_SetSRID(func.ST_MakePoint(0, 0), 4326)]))
    postgres_engine.execute(upd)


def _make_col(name, type, nullable):
    return Column(name, type, nullable=nullable)


def _copy_col(col):
    return _make_col(col.name, col.type, col.nullable)


class Creation(object):
    """
    When we're adding a dataset for the first time, create a brand new table
    """

    def __init__(self, staging, dataset):
        """
        :param staging: Table with data from CSV
        :param dataset: NamedTuple of dataset metadata
        """
        self.staging = staging
        self.dataset = dataset
        # Make a brand spanking new table
        self.table = self._init_table()
        # And insert data from an Update into it
        with Update(self.staging, self.dataset, self.table) as new:
            try:
                new.insert()
            except Exception as e:
                self.table.drop(bind=postgres_engine, checkfirst=True)
                raise e

    def _init_table(self):
        """
        Make a new table with the original columns from the staging table
        """
        # Take most columns straight from the source.
        original_cols = [_copy_col(c) for c in self.staging.columns
                         if c.name != 'hash']
        # Take care that the hash column is designated the primary key.
        original_cols.append(Column('hash', String(32), primary_key=True))

        # We also expect geometry and date columns to be created.
        derived_cols = [
            Column('point_date', TIMESTAMP, nullable=True, index=True),
            Column('geom', Geometry('POINT', srid=4326),
                   nullable=True, index=True)]
        new_table = Table(self.dataset.name, MetaData(),
                          *(original_cols + derived_cols))

        try:
            new_table.create(postgres_engine)
            # Trigger is broken
            #self._add_trigger()
        except:
            new_table.drop(bind=postgres_engine, checkfirst=True)
            raise
        else:
            return new_table

    def _add_trigger(self):
        add_trigger = """CREATE TRIGGER audit_after AFTER DELETE OR UPDATE
                         ON "{table}"
                         FOR EACH ROW EXECUTE PROCEDURE audit.if_modified()""".\
                      format(table=self.dataset.name)
        postgres_engine.execute(add_trigger)


class Update(object):
    """
    Create a table that contains the business key, geom, and date
    of all records found in the staging table and not in the existing table.
    """
    def __init__(self, staging, dataset, existing):
        """

        :param staging: Table full of CSV data.
        :param dataset: named tuple of type Dataset
        """
        self.staging = staging
        self.dataset = dataset
        self.existing = existing

        # We'll name it n_table
        self.name = 'n_' + dataset.name

        # This table will only have the hash
        # and the two derived columns for space and time.
        cols = [Column('hash', String(32), primary_key=True),
                _make_col('point_date', TIMESTAMP, True),
                _make_col('geom', Geometry('POINT', srid=4326), True)]

        self.table = Table(self.name, MetaData(), *cols)

    def __enter__(self):
        """
        Add a table (prefixed with n_) to the database
        with one record for each record found in the staging table
        with a hash not present in the existing table.
        If there are no such records, do nothing.
        """

        # create n_table with point_date, geom, and id columns
        s = self.staging
        e = self.existing
        d = self.dataset

        derived_dates = func.cast(s.c[d.date], TIMESTAMP).label('point_date')
        derived_geoms = self._geom_col()
        cols_to_insert = [s.c['hash'], derived_dates, derived_geoms]

        # Select the hash and the columns we're deriving from the staging table.
        sel = select(cols_to_insert)
        # And limit our results to records
        # whose hashes aren't already present in the existing table.
        sel = sel.select_from(s.outerjoin(e, s.c['hash'] == e.c['hash'])).\
            where(e.c['hash'] == None)

        # Drop the table first out of healthy paranoia
        self._drop()
        try:
            self.table.create(bind=postgres_engine)
        except Exception as e:
            raise PlenarioETLError(repr(e) +
                                   '\nCould not create table n_' + d.name)

        ins = self.table.insert().from_select(cols_to_insert, sel)
        # Populate it with records from our select statement.
        try:
            postgres_engine.execute(ins)
        except Exception as e:
            raise PlenarioETLError(repr(e) + '\n' + str(sel))
        else:
            # Would be nice to check if we have new records or not right here.
            return self

    def insert(self):
        """
        Join with the staging table
        to insert complete records into existing table.
        """
        derived_cols = [c for c in self.table.c
                        if c.name in {'geom', 'point_date'}]
        staging_cols = [c for c in self.staging.c]
        sel_cols = staging_cols + derived_cols

        sel = select(sel_cols).where(self.staging.c.hash == self.table.c.hash)
        ins = self.existing.insert().from_select(sel_cols, sel)

        try:
            postgres_engine.execute(ins)
        except Exception as e:
            raise PlenarioETLError(repr(e) +
                                   '\n Failed on statement: ' + str(ins))
        try:
            _null_malformed_geoms(self.existing)
        except Exception as e:
            raise PlenarioETLError(repr(e) +
                        '\n Failed to null out geoms with (0,0) geocoding')

    def _drop(self):
        postgres_engine.execute("DROP TABLE IF EXISTS {};".format(self.name))

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._drop()

    def _geom_col(self):
        """
        Derive selectable with a PostGIS point in 4326 projection
        derived from either the latitude and longitude columns
        or single location column
        """
        t = self.staging
        d = self.dataset

        if d.lat and d.lon:
            # Assume latitude and longitude columns are both numeric types.
            geom_col = func.ST_SetSRID(func.ST_Point(t.c[d.lon], t.c[d.lat]),
                                       4326).label('geom')

        elif d.loc:
            geom_col = func.point_from_loc(t.c[d.loc]).label('geom')

        else:
            msg = 'Staging table does not have geometry information.'
            raise PlenarioETLError(msg)

        return geom_col


def update_meta(metatable, table):
    """
    After ingest/update, update the metatable registry to reflect table information.

    :param metatable: MetaTable instance to update.
    :param table: Table instance to update from.

    :returns: None
    """

    with postgres_session_context() as session:
        metatable.update_date_added()

        metatable.obs_from, metatable.obs_to = session.query(
            func.min(table.c.point_date),
            func.max(table.c.point_date)
        ).first()

        metatable.bbox = session.query(
            func.ST_SetSRID(
                func.ST_Envelope(func.ST_Union(table.c.geom)),
                4326
            )
        ).first()[0]

        metatable.column_names = {
            c.name: str(c.type) for c in metatable.column_info()
            if c.name not in {'geom', 'point_date', 'hash'}
        }

        session.add(metatable)
