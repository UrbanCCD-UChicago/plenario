import os
from datetime import datetime
import gzip
from cStringIO import StringIO

import requests
from csvkit.unicsv import UnicodeCSVReader
from sqlalchemy import Boolean, Float, Date, String, Column, \
    Integer, Table, text, func, select, or_, and_, cast, UniqueConstraint, \
    join, outerjoin, BigInteger, MetaData
from sqlalchemy.dialects.postgresql import TIMESTAMP, ARRAY, TIME
from sqlalchemy.exc import NoSuchTableError
from geoalchemy2.shape import from_shape
from shapely.geometry import box
from boto.s3.connection import S3Connection, S3ResponseError
from boto.s3.key import Key

from plenario.database import task_session as session, task_engine as engine
from plenario.models import MetaTable, MasterTable
from plenario.utils.helpers import slugify, iter_column
from plenario.settings import AWS_ACCESS_KEY, AWS_SECRET_KEY, S3_BUCKET, DATA_DIR

COL_TYPES = {
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


class PlenarioETLError(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)
        self.message = message


class PlenarioETL(object):
    
    def __init__(self, meta, data_types=None):
        """ 
        Initializes with a dictionary representation of a
        row from the meta_master table.  If you include
        keys for all of the columns in the meta_master
        table, it doesn't hurt anything but the only keys
        that are required are:

        dataset_name:  Machine version of the dataset name.
                       This is used to name the primary key field of the
                       data table for the dataset as well as the table
                       itself.  Should be lowercase with words seperated
                       by underscores. Truncated to the first 50
                       characters.

        source_url:    This is used to download the raw data.

        business_key:  Name of the user identified business key from the
                       source data. AKA unique ID.

        observed_date: Name of the user identified observed date column 
                       from the source data

        latitude:      Name of the user identified latitude column
                       from the source data

        logitude:      Name of the user identified longitude column 
                       from the source data

        location:      Name of the user identified location column from
                       from the source data. The values in this column
                       should be formatted like so 
                       "(<latitude decimal degrees>, <longitude decimal degrees>)"

        You can also optionally supply a list of dicts with the names of the fields
        from the source data and the data type of the fields like so:

        [
          {
            'field_name': 'A field name',
            'data_type': 'integer',
          },
          {
            'field_name': 'Another field name',
            'data_type': 'string',
          },
          {
            'field_name': 'Last field name',
            'data_type': 'float'
          },
        ]

        'data_type' can be one of
          'boolean'
          'integer'
          'big_integer'
          'float'
          'string' 
          'date'
          'time'
          'timestamp'
        """

        # Add init parameters to PlenarioETL object
        for k,v in meta.items():
            setattr(self, k, v)

        if data_types:
            self.data_types = data_types

        self.s3_key = None

        self.metadata = MetaData()

        # AWS_ACCESS_KEY as empty string is signal to operate locally.
        if AWS_ACCESS_KEY != '':
            # Name of file in S3 bucket will be dataset name appended with current time.
            s3_path = '%s/%s.csv.gz' % (self.dataset_name, 
                                        datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))
            try:
                s3conn = S3Connection(AWS_ACCESS_KEY, AWS_SECRET_KEY)
                bucket = s3conn.get_bucket(S3_BUCKET)
                self.s3_key = Key(bucket)
                self.s3_key.key = s3_path
            except S3ResponseError, e:
                # XX: When this happens, we should log a more serious message
                print "Failed to connect to S3 for filename '%s', trying to init locally" % self.dataset_name
                self._init_local(self.dataset_name)                
        else:
            self._init_local(self.dataset_name)
    
    def _init_local(self, dataset_name):
        """
        Set directory to download and process data file as DATA_DIR/dataset_name.csv.gz
        """

        print "PlenarioETL._init_local('%s')" % dataset_name

        self.fname = '%s.csv.gz' % dataset_name
        self.data_dir = DATA_DIR

    def add(self, s3_path=None):
        if s3_path and s3_key:
            # It looks like s3_key is unresolved, so this branch should never be taken?
            self.s3_key.key = s3_path
        else:
            self._download_csv()
        self._get_or_create_data_table()
        self._make_src_table()
        self._insert_src_data()
        self._make_new_and_dup_table()
        self._find_dup_data()
        self._insert_new_data(added=True)
        self._insert_data_table()
        self._update_master()
        self._update_meta(added=True)
        self._update_geotags()
        self._cleanup_temp_tables()
    
    def update(self, s3_path=None):
        if s3_path and s3_key:
            self.s3_key.key = s3_path
        else:
            self._download_csv()
        self._get_or_create_data_table()
        self._make_src_table()
        self._insert_src_data()
        self._make_new_and_dup_table()
        self._find_dup_data()
        new = self._insert_new_data()
        if new:
            self._insert_data_table()
            self._update_master()
           #changes = self._find_changes()
           #if changes:
           #    self._update_dat_current_flag()
           #    self._update_master_current_flag()
        self._update_meta()
        self._update_geotags()
        self._cleanup_temp_tables()

    def _download_csv(self):
        """
        If self.s3_key is set, download CSV to S3 bucket.
        Else, download to local directory.
        """
        r = requests.get(self.source_url, stream=True)
 
        if self.s3_key:
            s = StringIO()
            with gzip.GzipFile(fileobj=s, mode='wb') as f:
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
                        f.flush()
            s.seek(0)
            self.s3_key.set_contents_from_file(s)
            self.s3_key.make_public()
        else:
            # write out a la shapefile_helpers
            self.fpath = os.path.join(self.data_dir, self.fname)

            # If file already exists locally, don't perform copy.
            if not os.path.exists(self.fpath):
                gz_f = gzip.open(self.fpath, 'wb')
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:
                        gz_f.write(chunk)
                        gz_f.flush()
                gz_f.close()  # Explicitly close before re-opening to read.
 
    def _cleanup_temp_tables(self):
        self.src_table.drop(bind=engine, checkfirst=True)
        self.new_table.drop(bind=engine, checkfirst=True)
        self.dup_table.drop(bind=engine, checkfirst=True)
        try:
            self.chg_table.drop(bind=engine, checkfirst=True)
        except AttributeError:
            pass

    def _get_or_create_data_table(self):
        """
        Step One: Make a table where the data will eventually live

        True after this function: self.dat_table refers to a (possibly empty)
        table in the database.
        """

        try:  # Maybe this table already exists in the database.
            self.dat_table = Table('dat_%s' % self.dataset_name, self.metadata, 
                                   autoload=True, autoload_with=engine, extend_existing=True)

        except NoSuchTableError:  # Nope, we'll need to create it.
            s = StringIO()

            # If reading from AWS...
            if self.s3_key:
                # ...dump the contents into s.
                self.s3_key.get_contents_to_file(s)
            # If reading locally...
            else:
                # ... read the file out of DATA_DIR.
                with open(self.fpath, 'r') as f:
                    s.write(f.read())

            # Go to start of file.
            s.seek(0)

            # Find out what types of columns we'll need to store the data.
            with gzip.GzipFile(fileobj=s, mode='rb') as f:
                reader = UnicodeCSVReader(f)
                header = map(slugify, reader.next())

                col_types = []  # Will be list of pairs: (column_type, is_nullable)

                try:  # Were data_types specified at init?
                    types = getattr(self, 'data_types')
                    col_map = {c['field_name']: c['data_type'] for c in types}
                    for col in header:
                        t = col_map[col]
                        col_types.append((COL_TYPES[t], True))  # always nullable

                except AttributeError:  # Try to infer column types.
                    for col in range(len(header)):
                        col_types.append(iter_column(col, f))

            # Create rows that will be used to keep track of the version of the source dataset
            # that each particular row came from.
            cols = [
                Column('%s_row_id' % self.dataset_name, Integer, primary_key=True),
                Column('start_date', TIMESTAMP, server_default=text('CURRENT_TIMESTAMP')),
                Column('end_date', TIMESTAMP, server_default=text('NULL')),
                Column('current_flag', Boolean, server_default=text('TRUE')),
                Column('dup_ver', Integer)
            ]

            # Generate columns for each column in the source dataset.
            for col_name,d_type in zip(header, col_types):
                dt, nullable = d_type
                cols.append(Column(col_name, dt, nullable=nullable))

            # Final column has columns whose values must be unique.
            # Generated from business_key, dup_ver, and dataset_name.
            cols.append(UniqueConstraint(slugify(self.business_key), 'dup_ver',
                                         name='%s_ix' % self.dataset_name[:50]))

            # Assemble data table from the columns...
            self.dat_table = Table('dat_%s' % self.dataset_name, self.metadata,
                                   *cols, extend_existing=True)
            # ... and load it into the database.
            self.dat_table.create(engine, checkfirst=True)

    def _make_src_table(self):
        """
        Step Two
        Creates a table that is a simple copy of the source data.
        (That is, it doesn't have the fancy extra columns like start_date that dat_table has)

        True after this function: self.src_table refers to an empty table in the database.
        """
        cols = []
        skip_cols = ['%s_row_id' % self.dataset_name, 'start_date', 'end_date', 'current_flag', 'dup_ver']
        for col in self.dat_table.columns:
            if col.name not in skip_cols:
                # Is there a default value that we need to give the coumn?
                kwargs = {}
                if col.server_default:
                    kwargs['server_default'] = col.server_default
                # Add the column as it was in dat_table
                cols.append(Column(col.name, col.type, **kwargs))

        # Use the source CSV's line numbers as primary key.
        cols.append(Column('line_num', Integer, primary_key=True))
        self.src_table = Table('src_%s' % self.dataset_name, self.metadata,
                               *cols, extend_existing=True)
        # If there is an old version of this raw dataset in the DB, kick it out.
        self.src_table.drop(bind=engine, checkfirst=True)
        # Add the table to the database.
        self.src_table.create(bind=engine)

    def _insert_src_data(self):
        """
        Step Three: Insert data directly from CSV

        True after this function: self.src_data is populated with the original dataset's values
        or a PlenarioETLError is triggered that brings the process to a halt.
        """

        skip_cols = ['line_num']
        names = [c.name for c in self.src_table.columns]

        # Create the COPY statement... creatively.
        copy_st = 'COPY src_%s (' % self.dataset_name
        for idx, name in enumerate(names):
            if name not in skip_cols:
                if idx < len(names) - len(skip_cols) - 1:
                    copy_st += '%s, ' % name
                else:
                    copy_st += '%s)' % name
            else:
                copy_st += "FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')"

        # Load the raw file from S3 or a local drive.
        s = StringIO()
        if self.s3_key:
            self.s3_key.get_contents_to_file(s)
        else:
            with open(self.fpath, 'r') as f:
                s.write(f.read())

        # Dump the contents into the src_table we've created.
        s.seek(0)
        conn = engine.raw_connection()
        with gzip.GzipFile(fileobj=s, mode='rb') as f:
            try:
                cursor = conn.cursor()
                cursor.copy_expert(copy_st, f)
                cursor.close()
                conn.commit()
            except Exception, e:  # When the bulk copy fails on _any_ row, roll back the entire operation.
                conn.rollback()
                raise PlenarioETLError(e)
            finally:
                conn.close()

        # The following code sets lat/lng to NULL when the given coordinate is (0,0) (e.g. off the coast of Africa).
        # This was a problem for: http://plenario-dev.s3.amazonaws.com/sfpd_incident_all_datetime.csv
        if self.latitude and self.longitude:        
            upd_st = """
                     UPDATE src_%s SET %s = NULL , %s = NULL FROM 
                     (SELECT %s FROM src_%s WHERE %s=0 and %s =0) AS ids 
                     WHERE src_%s.%s=ids.%s
                     """ % (self.dataset_name,
                            slugify(self.latitude), slugify(self.longitude),
                            slugify(self.business_key), self.dataset_name, slugify(self.latitude), slugify(self.longitude),
                            self.dataset_name, slugify(self.business_key),slugify(self.business_key))
            with engine.begin() as conn:
                conn.execute(upd_st)
        elif self.location:
            upd_st = """
                     UPDATE src_%s 
                     SET %s=NULL FROM                                     
                     (select %s,                                                                                              FLOAT8((regexp_matches(%s, '\((.*),(.*)\)'))[1]) as l1,
                     FLOAT8((regexp_matches(%s, '\((.*),(.*)\)'))[2]) as l2 
                     from  src_%s) as foo 
                     WHERE foo.l1=0 and foo.l2 = 0 AND src_%s.%s = foo.%s
                     """ % (self.dataset_name,
                            slugify(self.location),
                            slugify(self.business_key),
                            slugify(self.location),
                            slugify(self.location),
                            self.dataset_name,
                            self.dataset_name,slugify(self.business_key),slugify(self.business_key))
            with engine.begin() as conn:
                conn.execute(upd_st)

        # Also need to remove rows that have an empty business key
        # There might be a better way to do this...
        del_st = """ 
            DELETE FROM src_%s WHERE %s IS NULL
            """ % (self.dataset_name, slugify(self.business_key))
        with engine.begin() as conn:
            conn.execute(del_st)
            
    def _make_new_and_dup_table(self):
        """
        True after this function: self.new_table and self.dup_table
        are created with columns for line_num, dup_ver, and business_key.
        """
        # Grab the data table's business key column.
        bk_col = self.dat_table.c[slugify(self.business_key)]

        # TODO: DRY
        cols = [
            Column(slugify(self.business_key), bk_col.type, primary_key=True),
            Column('line_num', Integer),
            Column('dup_ver', Integer, primary_key=True)
        ]

        self.new_table = Table('new_%s' % self.dataset_name, self.metadata,
                               *cols, extend_existing=True)
        self.new_table.drop(bind=engine, checkfirst=True)
        self.new_table.create(bind=engine)
        
        cols = [
            Column(slugify(self.business_key), bk_col.type, primary_key=True),
            Column('line_num', Integer),
            Column('dup_ver', Integer, primary_key=True)
        ]
        self.dup_table = Table('dup_%s' % self.dataset_name, self.metadata,
                               *cols, extend_existing=True)
        self.dup_table.drop(bind=engine, checkfirst=True)
        self.dup_table.create(bind=engine)

    def _find_dup_data(self):
        """
        Step Five
        Construct dup_ver column of dup_table and populate dup_table.
        """

        # Taking the business key and line numbers of the source data...
        cols = [
            self.src_table.c[slugify(self.business_key)],
            self.src_table.c['line_num'],
        ]

        cols.append(func.rank()
                    # ... group by business key,
                    .over(partition_by=getattr(self.src_table.c, slugify(self.business_key)),
                          # ... and rank by line number.
                          order_by=self.src_table.columns['line_num'].desc())
                    # Call this our dup_ver column.
                    .label('dup_ver'))

        # Make these three columns our dup_table.
        sel = select(cols, from_obj=self.src_table)
        ins = self.dup_table.insert()\
            .from_select(self.dup_table.columns, sel)
        with engine.begin() as conn:
            conn.execute(ins)

    def _insert_new_data(self, added=False):
        """
        Step Six
        Find which rows in dup_table aren't present in dat_table.
        Add those new rows to new_table.
        """
        bk = slugify(self.business_key)

        # Align on line_num and bk (Shouldn't that include every entry of both tables?)
        j = join(self.src_table, self.dup_table, 
                 and_(self.src_table.c['line_num'] == self.dup_table.c['line_num'],
                      self.src_table.c[bk] == self.dup_table.c[bk]))

        dup_tablename = self.dup_table.name

        # Where possible, find where bk's and dup_ver's line up
        outer = outerjoin(j, self.dat_table,
                          and_(self.dat_table.c[bk] == j.c['%s_%s' % (dup_tablename, bk)],
                               self.dat_table.c['dup_ver'] == j.c['%s_dup_ver' % dup_tablename]))

        sel_cols = [
            self.src_table.c[bk],
            self.src_table.c['line_num'],
            self.dup_table.c['dup_ver']
        ]

        # If we are adding this dataset for the first time, bring in all of the deup_ver info
        sel = select(sel_cols).select_from(outer)

        if not added:  # If we are updating, (not adding)
            # only grab the dup_ver info not found in dat_table
            sel = sel.where(self.dat_table.c['%s_row_id' % self.dataset_name] == None)

        # Insert the new dup_ver info into new_table.
        ins = self.new_table.insert()\
            .from_select([c for c in self.new_table.columns], sel)
        try:
            with engine.begin() as conn:
                conn.execute(ins)
                return True
        except TypeError:
            # There are no new records
            return False

    def _insert_data_table(self):
        """
        Step Seven

        Insert the new data we identified in new_table into dat_table
        by joining the references in new_table to the actual data living in src_table.
        """
        # Take all columns from src_table (excluding most of the 'meta' columns)
        skip_cols = ['%s_row_id' % self.dataset_name,'end_date', 'current_flag', 'line_num']
        from_vals = []
        from_vals.append(text("'%s' AS start_date" % datetime.now().isoformat()))
        from_vals.append(self.new_table.c.dup_ver)

        for c_src in self.src_table.columns:
            if c_src.name not in skip_cols:
                from_vals.append(c_src)
        sel = select(from_vals, from_obj=self.src_table)

        bk = slugify(self.business_key)
        ins = self.dat_table.insert()\
            .from_select(
                [c for c in self.dat_table.columns if c.name not in skip_cols], 
                sel.select_from(self.src_table.join(self.new_table, 
                        and_(
                            self.src_table.c.line_num == self.new_table.c.line_num,
                            getattr(self.src_table.c, bk) == getattr(self.new_table.c, bk),
                        )
                    ))
            )
        with engine.begin() as conn:
            conn.execute(ins)

    def _update_master(self, added=False):
        """
        Step Eight: Insert new records into master table
        """

        # Enumerate all the columns we'll be populating from dat_table
        dat_cols = [
            self.dat_table.c.start_date,
            self.dat_table.c.end_date,
            self.dat_table.c.current_flag,
        ]
        if self.location:
            dat_cols.append(getattr(self.dat_table.c, slugify(self.location))\
                .label('location'))
        else:
            dat_cols.append(text("NULL as location"))
        if self.latitude and self.longitude:
            dat_cols.append(getattr(self.dat_table.c, slugify(self.latitude))\
                .label('latitude'))
            dat_cols.append(getattr(self.dat_table.c, slugify(self.longitude))\
                .label('longitude'))
        else:
            dat_cols.append(text("NULL AS latitude"))
            dat_cols.append(text("NULL AS longitude"))
        dat_cols.append(func.cast(getattr(self.dat_table.c, slugify(self.observed_date)), TIMESTAMP)\
            .label('obs_date'))
        dat_cols.append(text("NULL AS weather_station_id"))
        dat_cols.append(text("NULL AS geotag2"))
        dat_cols.append(text("NULL AS geotag3"))
        dat_cols.append(text("'%s' AS dataset_name" % self.dataset_name))
        dat_pk = '%s_row_id' % self.dataset_name
        dat_cols.append(getattr(self.dat_table.c, dat_pk))

        # Derive point in space from either lat/long columns or single location column
        if self.latitude and self.longitude:
            dat_cols.append(text(
                "ST_PointFromText('POINT(' || dat_%s.%s || ' ' || dat_%s.%s || ')', 4326) \
                      as location_geom" % (
                          self.dataset_name, slugify(self.longitude), 
                          self.dataset_name, slugify(self.latitude),
                      )))
        elif self.location:
            dat_cols.append(text(
                """ (
                    SELECT ST_PointFromText('POINT(' || subq.longitude || ' ' || subq.latitude || ')', 4326) \
                        FROM (
                              SELECT FLOAT8((regexp_matches(%s, '\((.*),.*\)'))[1]) AS latitude, \
                                     FLOAT8((regexp_matches(%s, '\(.*,(.*)\)'))[1]) AS longitude \
                              FROM dat_%s as d where d."%s" = dat_%s."%s") AS subq) AS location_geom
                """ % 
                      (
                          slugify(self.location), slugify(self.location),
                          self.dataset_name, dat_pk, self.dataset_name, dat_pk,
                      )))

        # Insert the data
        mt = MasterTable.__table__
        bk = slugify(self.business_key)

        # If we're adding the dataset for the first time,
        if added:
            # just throw everything in.
            ins = mt.insert()\
                .from_select(
                    [c for c in mt.columns.keys() if c != 'master_row_id'], 
                    select(dat_cols)
                )
        else:  # If we're updating,
            # just add the new stuff by joining on new_table.
            ins = mt.insert()\
                .from_select(
                    [c for c in mt.columns.keys() if c != 'master_row_id'],
                    select(dat_cols)\
                        .select_from(self.dat_table.join(self.new_table, 
                            and_(
                                getattr(self.dat_table.c, bk) == getattr(self.new_table.c, bk),
                                self.dat_table.c.dup_ver == self.new_table.c.dup_ver
                            )
                        )
                    )
                )
        with engine.begin() as conn:
            conn.execute(ins)

    def _add_weather_info(self):
        """ 
        This is just adding the weather observation id to the master table right now.
        In the future we can modify it to do all the geo tagging we need for the
        master table.

        The update below assumes the weather stations table has already been
        created and populated. I have no idea how to do it in SQLAlchemy, 
        mainly because of the geometry distance operator ('<->')
        """

        # Yo dawg, I heard you like subqueries. 
        # I put a subquery in your subquery.
        date_type = str(getattr(self.dat_table.c, slugify(self.observed_date)).type)
        if 'timestamp' in date_type.lower():
            weather_table = 'dat_weather_observations_hourly'
            date_col_name = 'datetime'
            temp_col = 'drybulb_fahrenheit'
        else:
            weather_table = 'dat_weather_observations_daily'
            date_col_name = 'date'
            temp_col = 'temp_avg'
        upd = text(
            """
            UPDATE dat_master SET weather_observation_id=subq.weather_id
                FROM (
                SELECT DISTINCT ON (d.master_row_id) 
                d.master_row_id AS master_id, 
                w.id as weather_id, 
                abs(extract(epoch from d.obs_date) - extract(epoch from w.%s)) as diff 
                FROM dat_master AS d 
                JOIN %s as w 
                  ON w.wban_code = (
                    SELECT b.wban_code 
                      FROM weather_stations AS b 
                      ORDER BY d.location_geom <-> b.location LIMIT 1
                    ) 
                WHERE d.location_geom IS NOT NULL 
                  AND d.weather_observation_id IS NULL 
                  AND d.dataset_name = :dname 
                  AND d.obs_date > (
                    SELECT MIN(%s) 
                      FROM %s 
                      WHERE %s IS NOT NULL
                    ) 
                  AND d.obs_date < (
                    SELECT MAX(%s) 
                      FROM %s 
                      WHERE %s IS NOT NULL
                    ) 
                ORDER BY d.master_row_id, diff
              ) as subq
            WHERE dat_master.master_row_id = subq.master_id
            """ % (date_col_name, weather_table, 
                   date_col_name, weather_table, temp_col,
                   date_col_name, weather_table, temp_col,)
        )
        with engine.begin() as conn:
            conn.execute(upd, dname=self.dataset_name)
        
    def _add_census_block(self):
        """ 
        Adds a census block geoid to entries in the master table
        """
        upd = text(""" 
            UPDATE dat_master SET census_block=subq.census_block
                FROM (
                    SELECT 
                        d.master_row_id as master_id,
                        c.geoid10 as census_block
                    FROM
                       dat_master as d
                    JOIN census_blocks as c
                       ON ST_Within(d.location_geom, c.geom)
                    WHERE d.census_block IS NULL
                        AND d.location_geom IS NOT NULL
                        AND d.dataset_name = :dname
                ) as subq
                WHERE dat_master.master_row_id = subq.master_id
            """)
        with engine.begin() as conn:
            conn.execute(upd, dname=self.dataset_name)

    def _update_geotags(self):
        # self._add_weather_info()
        # self._add_weather_stations()
        self._add_census_block()

    def _update_meta(self, added=False):
        """ 
        Update the meta_master table with obs_from, obs_to, 
        updated_date, bbox, and (when appropriate) date_added
        """
        md = session.query(MetaTable)\
            .filter(MetaTable.source_url_hash == self.source_url_hash)\
            .first()

        # Update time columns
        now = datetime.now()
        md.last_update = now
        if added:
            md.date_added = now
        obs_date_col = getattr(self.dat_table.c, slugify(self.observed_date))
        obs_from, obs_to = session.query(
                               func.min(obs_date_col), 
                               func.max(obs_date_col))\
                               .first()
        md.obs_from = obs_from
        md.obs_to = obs_to

        # Calculate bounding box
        if self.latitude and self.longitude:
            lat_col = getattr(self.dat_table.c, slugify(self.latitude))
            lon_col = getattr(self.dat_table.c, slugify(self.longitude))
            xmin, ymin, xmax, ymax = session.query(
                                         func.min(lon_col),
                                         func.min(lat_col),
                                         func.max(lon_col),
                                         func.max(lat_col))\
                                         .first()
        elif self.location:
            loc_col = getattr(self.dat_table.c, slugify(self.location))
            subq = session.query(
                cast(func.regexp_matches(loc_col, '\((.*),.*\)'), 
                    ARRAY(Float)).label('lat'), 
                cast(func.regexp_matches(loc_col, '\(.*,(.*)\)'), 
                    ARRAY(Float)).label('lon'))\
                .subquery()
            try:
                xmin, ymin, xmax, ymax = session.query(func.min(subq.c.lon), 
                                                       func.min(subq.c.lat), 
                                                       func.max(subq.c.lon), 
                                                       func.max(subq.c.lat))\
                                                .first()
                xmin, ymin, xmax, ymax = xmin[0], ymin[0], xmax[0], ymax[0]
            except:
                session.rollback()
                xmin, ymin, xmax, ymax = 0, 0, 0, 0
        bbox = from_shape(box(xmin, ymin, xmax, ymax), srid=4326)
        md.bbox = bbox
        try:
            session.add(md)
            session.commit()
        except:
            session.rollback()
            session.add(md)
            session.commit()
