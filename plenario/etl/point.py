from plenario.etl.common import ETLFile
from csvkit.unicsv import UnicodeCSVReader
from sqlalchemy.exc import NoSuchTableError
from plenario.database import app_engine as engine, session
from plenario.utils.helpers import iter_column, slugify
import json
from sqlalchemy import Boolean, Integer, BigInteger, Float, String, Date, TIME, TIMESTAMP,\
    Table, Column, MetaData
from sqlalchemy import select, func, text
from geoalchemy2 import Geometry
from plenario.models import MetaTable
from plenario.etl.common import PlenarioETLError


class PlenarioETL(object):
    def __init__(self, metadata, source_path=None):
        """
        :param metadata: MetaTable instance of dataset being ETL'd.
        :param source_path: If provided, get source CSV ftom local filesystem instead of URL in metadata.
        """
        self.metadata = metadata
        self.staging_table = StagingTable(self.metadata, source_path=source_path)

    def add(self):
        """
        Create point table for the first time.
        """
        with self.staging_table as s_table:
            new_table = s_table.create_new()
        update_meta(new_table)

    def update(self):
        """
        Insert new records into existing point table.
        """
        existing_table = self.metadata.point_table
        with self.staging_table as s_table:
            s_table.insert_into(existing_table)
        update_meta(existing_table)


class StagingTable(object):
    def __init__(self, meta, source_path=None):
        """
        A temporary table that will contain all records from the source CSV.
        From it, either create a new point table
        or insert records from it into an existing point table.

        :param meta: record from MetaTable
        :param source_path: path of source file on local filesystem
        """
        self.meta = meta
        # The sqlalchemy metadata registry in which we must register our staging table.
        self.sa_meta = MetaData()

        # Get the Columns to construct our table
        try:
            self.cols = self._from_ingested()
        except NoSuchTableError:
            # This must be the first time we're ingesting the table
            if meta.contributed_data_types:
                types = json.loads(meta.contributed_data_types)
                self.cols = self._from_contributed(types)
            else:
                self.cols = None

        # Retrieve the source file
        try:
            if source_path:  # Local ingest
                self.file_helper = ETLFile(source_path=source_path)
            else:  # Remote ingest
                self.file_helper = ETLFile(source_url=meta.source_url)
        # TODO: Handle more specific exception
        except Exception as e:
            raise PlenarioETLError(e)

    def __enter__(self):
        """
        Create the staging table. Will be named s_[dataset_name]
        """
        with self.file_helper as helper:
            if not self.cols:
                # We couldn't get the column metadata from an existing table or from the user.
                self.cols = self._from_inference(helper.handle)

            # Grab the handle to build a table from the CSV
            try:
                self.table = self._make_table(helper.handle)
                self._kill_dups()
                return self
            except Exception as e:
                # Some stuff that could happen:
                    # There could be more columns in the source file than we expected.
                    # Some input could be malformed.
                raise PlenarioETLError(e)

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Drop the staging table if it's been created.
        """
        session.close()
        if hasattr(self, 'table'):
            self.table.drop(bind=engine, checkfirst=True)
        else:
            # If the copy operation fails during _make_table,
            # then the `table` variable won't have been assigned to yet.
            brute_force = "DROP TABLE IF EXISTS {};".format('s_' + self.meta.dataset_name)
            engine.execute(brute_force)

        # Let the exception information (exc_type et al.) propagate up and get reported in the ETL log.

    def _make_table(self, f):
        # Persist an empty table eagerly
        # so that we can access it when we drop down to a raw connection.
        s_table_name = 's_' + self.meta.dataset_name

        # Make a sequential primary key to track line numbers
        self.cols.append(Column('line_num', Integer, primary_key=True))

        table = Table(s_table_name, self.sa_meta, *self.cols, extend_existing=True)

        # Be paranoid and remove the table if one by this name already exists.
        table.drop(bind=engine, checkfirst=True)
        table.create(bind=engine)

        # Fill in the columns we expect from the CSV.
        names = [c.name for c in self.cols if c.name != 'line_num']
        copy_st = "COPY {t_name} ({cols}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')".\
            format(t_name=s_table_name, cols=', '.join(names))

        # In order to issue a COPY, we need to drop down to the psycopg2 DBAPI.
        conn = engine.raw_connection()
        try:
            with conn.cursor() as cursor:
                cursor.copy_expert(copy_st, f)
                conn.commit()
                return table
        except Exception as e:  # When the bulk copy fails on _any_ row, roll back the entire operation.
            raise PlenarioETLError(e)
        finally:
            conn.close()

    @staticmethod
    def _null_malformed_geoms(existing):
        # We decide to set the geom to NULL when the given lon/lat is (0,0) (e.g. off the coast of Africa).
        upd = existing.update().values(geom=None).\
            where(existing.c.geom == select([func.ST_SetSRID(func.ST_MakePoint(0, 0), 4326)]))
        engine.execute(upd)

    def _kill_dups(self):
        # When a unique ID is duplicated, only retain the record with that ID found highest in the source file.
        t = self.table
        del_stmt = '''
        DELETE FROM {table}
        WHERE line_num NOT IN (
          SELECT MIN(line_num) FROM {table}
          GROUP BY {bk}
        )
        '''.format(table=t.name, bk=self.meta.business_key)

        session.execute(del_stmt)
        try:
            session.commit()
        except:
            session.rollback()
            raise

    @staticmethod
    def _make_col(name, type, nullable):
        return Column(name, type, nullable=nullable)

    def _copy_col(self, col):
        return self._make_col(col.name, col.type, col.nullable)

    '''Utility methods to generate columns into which we can dump the CSV data.'''
    def _from_ingested(self):
        """
        Generate columns from the existing table.
        """
        ingested_cols = self.meta.column_info()
        # Don't include the geom and point_date columns.
        # They're derived from the source data and won't be present in the source CSV
        original_cols = [c for c in ingested_cols if c.name not in ['geom', 'point_date']]
        # Make copies that don't refer to the existing table.
        cols = [self._copy_col(c) for c in original_cols]

        return cols

    def _from_inference(self, f):
        """
        Generate columns by scanning source CSV and inferring column types.
        """
        reader = UnicodeCSVReader(f)
        # Always create columns with slugified names
        header = map(slugify, reader.next())

        cols = []
        for col_idx, col_name in enumerate(header):
            col_type, nullable = iter_column(col_idx, f)
            cols.append(self._make_col(col_name, col_type, nullable))
        return cols

    def _from_contributed(self, data_types):
        """
        :param data_types: List of dictionaries, each of which has 'field_name' and 'data_type' fields.

        Generate columns from user-given specifications.
        (Warning: assumes user has completely specified every column.
        We don't support mixing inferred and user-specified columns.)
        """
        # The keys in this mapping are taken from the frontend form
        # where users can specify column types.
        col_types = {
            'boolean': Boolean,
            'integer': Integer,
            'big_integer': BigInteger,
            'float': Float,
            'string': String,
            'date': Date,
            'time': TIME,
            'timestamp': TIMESTAMP,
            'datetime': TIMESTAMP,
        }

        cols = [self._make_col(c['field_name'], col_types[c['data_type']], True) for c in data_types]
        return cols

    '''Construct derived columns for the canonical table.'''

    def _date_selectable(self):
        """
        Make a selectable where we take the dataset's temporal column
        And cast every record to a Postgres TIMESTAMP
        """

        return func.cast(self.table.c[self.meta.observed_date], TIMESTAMP).\
                label('point_date')

    def _geom_selectable(self):
        """
        Derive selectable with a PostGIS point in 4326 (naive lon-lat) projection
        derived from either the latitude and longitude columns or single location column
        """
        t = self.table
        m = self.meta

        if m.latitude and m.longitude:
            geom_col = func.ST_SetSRID(func.ST_Point(t.c[m.longitude], t.c[m.latitude]),
                                       4326).label('geom')

        elif m.location:
            # I had trouble handling postgres arrays in SQLAlchemy, so I used raw SQL here.
            # (The two capture groups from the regex get returned as an array)
            # (Also - NB - postgres arrays are 1-indexed!)
            geom_col = text(
                    '''SELECT ST_PointFromText('POINT(' || subq.lon || ' ' || subq.lat || ')', 4326) \
                          FROM (SELECT a[1] AS lon, a[2] AS lat
                                  FROM (SELECT regexp_matches({}, '\((.*), (.*)\)') FROM {} AS FLOAT8(a))
                                AS subq)
                       AS geom;'''.format(t.c[m.location], 's_' + m.dataset_name))
        else:
            raise PlenarioETLError('Staging table does not have geometry information.')

        return geom_col

    def _new_records(self, existing):
        """
        Find all records with unique ids not present in the existing table/
        """
        t = self.table
        m = self.meta

        # The select_from and where clauses ensure we're only looking at records
        # that don't have a unique ID that's present in the existing dataset.
        #
        # Finally, include the id itself in the common table expression to join to the staging table.
        sel = select([t.c[m.business_key].label('id')]).\
            select_from(t.outerjoin(existing, t.c[m.business_key] == existing.c[m.business_key])).\
            where(existing.c[m.business_key] == None).\
            distinct().\
            alias('new')

        return sel

    def create_new(self):
        """
        Make a new table and insert every record from the staging table into it.
        """
        # Take most columns straight from the source.
        verbatim_cols = []
        for c in self.cols:
            if c.name == 'line_num':
                # We only created line_num to deduplicate. Don't include it in our canonical table.
                continue
            elif c.name == self.meta.business_key:
                # The business_key will be our unique ID
                verbatim_cols.append(Column(c.name, c.type, primary_key=True))
            else:
                verbatim_cols.append(self._copy_col(c))

        # Create geometry and timestamp columns
        derived_cols = [
            Column('point_date', TIMESTAMP, nullable=False, index=True),
            Column('geom', Geometry('POINT', srid=4326), nullable=True, index=True)
        ]

        new_table = Table(self.meta.dataset_name, self.sa_meta, *(verbatim_cols + derived_cols))
        new_table.create(engine)

        # Ask the staging table to insert its new columns into the newly created table.
        self.insert_into(new_table)
        return new_table

    def insert_into(self, existing):
        """
        Insert new columns from staging table into a table that already exists in Plenario.

        :param existing: Point table that has been persisted to the DB.
        """
        new = self._new_records(existing)
        # Insert into the canonical table the original cols
        ins_cols = [c for c in self.cols if c.name != 'line_num']
        # ... and columns that we'll derive from the original columns.
        geom_sel, date_sel = self._geom_selectable(), self._date_selectable()
        ins_cols += [geom_sel, date_sel]

        # The `new` subquery only includes rows with business keys that weren't present in the existing table.
        sel = select(ins_cols).\
            select_from(new.join(self.table, new.c.id == self.table.c[self.meta.business_key])).\
            where(date_sel != None)  # Also, filter out rows with a null date.

        ins = existing.insert().from_select(ins_cols, sel)
        try:
            engine.execute(ins)
        except TypeError:
            # We get a TypeError if there are no record found in the select statement.
            # In which case, great! Our job is done.
            return
        except Exception as e:
            raise PlenarioETLError(e)

        self._null_malformed_geoms(existing)


def update_meta(table):
    """
    After ingest/update, update the metadata registry to reflect
    :param table:
    """
    record = MetaTable.get_by_dataset_name(table.name)
    record.update_date_added()
    record.obs_from, record.obs_to = session.query(func.min(table.c.point_date),
                                                   func.max(table.c.point_date)).first()
    bbox = session.query(func.ST_SetSRID(
                                         func.ST_Envelope(func.ST_Union(table.c.geom)),
                                         4326
                                         )).first()[0]
    record.bbox = bbox
    session.add(record)
    try:
        session.commit()
    # TODO: Catch more specific
    except:
        session.rollback()
        raise
