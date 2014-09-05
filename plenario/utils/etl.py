import requests
import re
import os
from datetime import datetime, date, time
from plenario.database import task_session as session, task_engine as engine, \
    Base
from plenario.models import MetaTable, MasterTable
from plenario.utils.helpers import slugify
from plenario.settings import AWS_ACCESS_KEY, AWS_SECRET_KEY, S3_BUCKET
from urlparse import urlparse
from csvkit.unicsv import UnicodeCSVReader
from plenario.utils.typeinference import normalize_column_type
import gzip
from sqlalchemy import Boolean, Float, DateTime, Date, Time, String, Column, \
    Integer, Table, text, func, select, or_, and_, cast, UniqueConstraint, \
    join, outerjoin, over
from sqlalchemy.dialects.postgresql import TIMESTAMP, ARRAY
from sqlalchemy.exc import NoSuchTableError
from types import NoneType
import plenario.settings
from geoalchemy2.shape import from_shape
from shapely.geometry import box
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from cStringIO import StringIO

class PlenarioETL(object):
    
    def __init__(self, meta):
        """ 
        Initializes with a dictionary representation of a
        row from the meta_master table.  If you include
        keys for all of the columns in the meta_master
        table, it doen't hurt anything but the only keys
        that are required are:

        dataset_name:  Machine version of the dataset name.
                       This is used to name the primary key field of the
                       data table for the dataset as well as the table
                       itself.  Should be lowercase with words seperated
                       by underscores. Truncated to the first 50
                       characters.

        source_url:    This is used to download the raw data.

        business_key:  Name of the user identified business key from the
                       source data.

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
        """
        for k,v in meta.items():
            setattr(self, k, v)
        domain = urlparse(self.source_url).netloc
        fourbyfour = self.source_url.split('/')[-1]
        self.view_url = 'http://%s/api/views/%s' % (domain, fourbyfour)
        self.dl_url = '%s/rows.csv?accessType=DOWNLOAD' % self.view_url
        s3_path = '%s/%s.csv.gz' % (self.dataset_name, 
            datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))
        s3conn = S3Connection(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        bucket = s3conn.get_bucket(S3_BUCKET)
        self.s3_key = Key(bucket)
        self.s3_key.key = s3_path
    
    def _get_tables(self, table_name=None, all_tables=False):
        if all_tables:
            table_names = ['src', 'dup', 'new', 'dat']
            for table in table_names:
                try:
                    t = Table('%s_%s' % (table, self.dataset_name), Base.metadata,
                        autoload=True, autoload_with=engine, extend_existing=True)
                    setattr(self, '%s_table' % table, t)
                except NoSuchTableError:
                    pass
        else:
            try:
                t = Table('%s_%s' % (table_name, self.dataset_name), Base.metadata,
                    autoload=True, autoload_with=engine, extend_existing=True)
                setattr(self, '%s_table' % table_name, t)
            except NoSuchTableError:
                pass

    def add(self, s3_path=None):
        if s3_path:
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
        if s3_path:
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
        r = requests.get(self.dl_url, stream=True)
        s = StringIO()
        with gzip.GzipFile(fileobj=s, mode='wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    f.flush()
        s.seek(0)
        self.s3_key.set_contents_from_file(s)
        self.s3_key.make_public()
 
    def _cleanup_temp_tables(self):
        self.src_table.drop(bind=engine, checkfirst=True)
        self.new_table.drop(bind=engine, checkfirst=True)
        self.dup_table.drop(bind=engine, checkfirst=True)
        try:
            self.chg_table.drop(bind=engine, checkfirst=True)
        except AttributeError:
            pass

    def iter_column(self, idx, f):
        f.seek(0)
        reader = UnicodeCSVReader(f)
        header = reader.next()
        col = []
        for row in reader:
            col.append(row[idx])
        col_type = normalize_column_type(col)
        return col_type

    def _get_or_create_data_table(self):
        # Step One: Make a table where the data will eventually live
        try:
            self.dat_table = Table('dat_%s' % self.dataset_name, Base.metadata, 
                autoload=True, autoload_with=engine, extend_existing=True)
        except NoSuchTableError:
            s = StringIO()
            self.s3_key.get_contents_to_file(s)
            s.seek(0)
            col_types = []
            with gzip.GzipFile(fileobj=s, mode='rb') as f:
                reader = UnicodeCSVReader(f)
                header = reader.next()
                for col in range(len(header)):
                    col_types.append(self.iter_column(col, f))
            cols = [
                Column('%s_row_id' % self.dataset_name, Integer, primary_key=True),
                Column('start_date', TIMESTAMP, server_default=text('CURRENT_TIMESTAMP')),
                Column('end_date', TIMESTAMP, server_default=text('NULL')),
                Column('current_flag', Boolean, server_default=text('TRUE')),
                Column('dup_ver', Integer)
            ]
            for col_name,d_type in zip(header, col_types):
                cols.append(Column(slugify(col_name), d_type))
            cols.append(UniqueConstraint(slugify(self.business_key), 'dup_ver', 
                    name='%s_ix' % self.dataset_name[:50]))
            self.dat_table = Table('dat_%s' % self.dataset_name, Base.metadata, 
                          *cols, extend_existing=True)
            self.dat_table.create(engine, checkfirst=True)

    def _make_src_table(self):
        # Step Two
        cols = []
        skip_cols = ['%s_row_id' % self.dataset_name, 'start_date', 'end_date', 'current_flag', 'dup_ver']
        for col in self.dat_table.columns:
            if col.name not in skip_cols:
                kwargs = {}
                if col.server_default:
                    kwargs['server_default'] = col.server_default
                cols.append(Column(col.name, col.type, **kwargs))
        cols.append(Column('line_num', Integer, primary_key=True))
        self.src_table = Table('src_%s' % self.dataset_name, Base.metadata,
                          *cols, extend_existing=True)
        self.src_table.drop(bind=engine, checkfirst=True)
        self.src_table.create(bind=engine)

    def _insert_src_data(self):
        # Step Three: Insert data directly from CSV
        cols = []
        skip_cols = ['line_num']
        names = [c.name for c in self.src_table.columns]
        copy_st = 'COPY src_%s (' % self.dataset_name
        for idx, name in enumerate(names):
            if name not in skip_cols:
                if idx < len(names) - len(skip_cols) - 1:
                    copy_st += '%s, ' % name
                else:
                    copy_st += '%s)' % name
        else:
            copy_st += "FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')"
        conn = engine.raw_connection()
        cursor = conn.cursor()
        s = StringIO()
        self.s3_key.get_contents_to_file(s)
        s.seek(0)
        with gzip.GzipFile(fileobj=s, mode='rb') as f:
            cursor.copy_expert(copy_st, f)
        conn.commit()

    def _make_new_and_dup_table(self):
        # Step Four
        bk_col = self.dat_table.c[slugify(self.business_key)]
        cols = [
            Column(slugify(self.business_key), bk_col.type, primary_key=True),
            Column('line_num', Integer),
            Column('dup_ver', Integer, primary_key=True)
        ]
        self.new_table = Table('new_%s' % self.dataset_name, Base.metadata,
            *cols, extend_existing=True)
        self.new_table.drop(bind=engine, checkfirst=True)
        self.new_table.create(bind=engine)
        
        cols = [
            Column(slugify(self.business_key), bk_col.type, primary_key=True),
            Column('line_num', Integer),
            Column('dup_ver', Integer, primary_key=True)
        ]
        self.dup_table = Table('dup_%s' % self.dataset_name, Base.metadata,
            *cols, extend_existing=True)
        self.dup_table.drop(bind=engine, checkfirst=True)
        self.dup_table.create(bind=engine)

    def _find_dup_data(self):
        # Step Five
        cols = [
            self.src_table.c[slugify(self.business_key)],
            self.src_table.c['line_num'],
        ]
        cols.append(func.rank()\
            .over(partition_by=getattr(self.src_table.c, slugify(self.business_key)), 
                order_by=self.src_table.columns['line_num'].desc())\
            .label('dup_ver'))
        sel = select(cols, from_obj=self.src_table)
        ins = self.dup_table.insert()\
            .from_select([c for c in self.dup_table.columns], sel)
        conn = engine.connect()
        conn.execute(ins)

    def _insert_new_data(self, added=False):
        # Step Six
        bk = slugify(self.business_key)
        sel_cols = [
            self.src_table.c[bk], 
            self.src_table.c['line_num'], 
            self.dup_table.c['dup_ver']
        ]
        j = join(self.src_table, self.dup_table, 
            and_(self.src_table.c['line_num'] == self.dup_table.c['line_num'], 
                self.src_table.c[bk] == self.dup_table.c[bk]))
        dup_tablename = self.dup_table.name
        outer = outerjoin(j, self.dat_table, 
              and_(self.dat_table.c[bk] == j.c['%s_%s' % (dup_tablename, bk)], 
                   self.dat_table.c['dup_ver'] == j.c['%s_dup_ver' % dup_tablename]))
        sel = select(sel_cols).select_from(outer)
        if not added:
            sel = sel.where(self.dat_table.c['%s_row_id' % self.dataset_name] == None)
        ins = self.new_table.insert()\
            .from_select([c for c in self.new_table.columns], sel)
        conn = engine.connect()
        try:
            conn.execute(ins)
            return True
        except TypeError:
            # There are no new records
            return False

    def _insert_data_table(self):
        # Step Seven
        bk = slugify(self.business_key)
        skip_cols = ['%s_row_id' % self.dataset_name,'end_date', 'current_flag', 'line_num']
        from_vals = []
        from_vals.append(text("'%s' AS start_date" % datetime.now().isoformat()))
        from_vals.append(self.new_table.c.dup_ver)
        for c_src in self.src_table.columns:
            if c_src.name not in skip_cols:
                from_vals.append(c_src)
        sel = select(from_vals, from_obj = self.src_table)
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
        conn = engine.connect()
        conn.execute(ins)

    def _update_master(self, added=False):
        # Step Eight: Insert new records into master table
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
        dat_cols.append(getattr(self.dat_table.c, slugify(self.observed_date))\
            .label('obs_date'))
        dat_cols.append(text("NULL AS weather_station_id"))
        dat_cols.append(text("NULL AS geotag2"))
        dat_cols.append(text("NULL AS geotag3"))
        dat_cols.append(text("'%s' AS dataset_name" % self.dataset_name))
        dat_cols.append(getattr(self.dat_table.c, '%s_row_id' % self.dataset_name))
        if self.latitude and self.longitude:
            dat_cols.append(text(
                "ST_PointFromText('POINT(' || dat_%s.%s || ' ' || dat_%s.%s || ')', 4326) \
                      as location_geom" % (
                          self.dataset_name, self.longitude, 
                          self.dataset_name, self.latitude,
                      )))
        elif self.location:
            # probably a better way to do this...
            dat_cols.append(text(
                "ST_PointFromText('POINT(' || \
                      split_part(substr(replace(dat_%s.%s, ')', ''), strpos(dat_%s.%s, '(') + 1, length(dat_%s.%s) - 2), ',', 2) \
                      || ' ' || \
                      split_part(substr(replace(dat_%s.%s, '(', ''), strpos(dat_%s.%s, '(') + 1, length(dat_%s.%s) - 2), ',', 1) \
                      || ')', 4326) \
                      as location_geom" % (
                          self.dataset_name, slugify(self.location), 
                          self.dataset_name, slugify(self.location),
                          self.dataset_name, slugify(self.location), 
                          self.dataset_name, slugify(self.location),
                          self.dataset_name, slugify(self.location), 
                          self.dataset_name, slugify(self.location),
                      )))
        mt = MasterTable.__table__
        bk = slugify(self.business_key)
        if added:
            ins = mt.insert()\
                .from_select(
                    [c for c in mt.columns.keys() if c != 'master_row_id'], 
                    select(dat_cols)
                )
        else:
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
        conn = engine.connect()
        conn.execute(ins)

    def _update_geotags(self):
        """ 
        This is just adding the weather station id to the master table right now.
        In the future we can modify it to do all the geo tagging we need for the
        master table.

        The update below assumes the weather stations table has already been
        created and populated. I have no idea how to do it in SQLAlchemy, 
        mainly because of the geometry distance operator ('<->')
        """

        # Yo dawg, I heard you like subqueries. 
        # I put a subquery in your subquery.
        upd = text(
            """ 
            UPDATE dat_master SET weather_station_id=subq.wban_code 
                FROM (
                    SELECT a.master_row_id as row_id, (
                        SELECT b.wban_code 
                            FROM weather_stations AS b 
                            ORDER BY a.location_geom <-> b.location 
                            LIMIT 1
                        ) AS wban_code 
                        FROM dat_master AS a
                    ) AS subq 
                WHERE dat_master.master_row_id = subq.row_id 
                    AND dat_master.location_geom IS NOT NULL
                    AND dat_master.weather_station_id IS NULL
            """
        )
        conn = engine.connect()
        conn.execute(upd)

    def _find_changes(self):
        # Step Eight: Find changes
        bk = slugify(self.business_key)
        self.chg_table = Table('chg_%s' % self.dataset_name, Base.metadata,
                      Column('id', Integer), 
                      extend_existing=True)
        self.chg_table.drop(bind=engine, checkfirst=True)
        self.chg_table.create(bind=engine)
        bk = slugify(self.business_key)
        skip_cols = ['start_date', 'end_date', 'current_flag', bk, 
            '%s_row_id' % self.dataset_name, 'dup_ver']
        src_cols = [c for c in self.src_table.columns if c.name != bk]
        dat_cols = [c for c in self.dat_table.columns if c.name not in skip_cols]
        and_args = []
        for s,d in zip(src_cols, dat_cols):
            ors = or_(s != None, d != None)
            ands = and_(ors, s != d)
            and_args.append(ands)
        pk = getattr(self.dat_table.c, '%s_row_id' % self.dataset_name)
        ins = self.chg_table.insert()\
            .from_select(
                ['id'],
                select([pk])\
                    .select_from(
                        join(self.dat_table, self.src_table, 
                            self.dat_table.c.service_request_number == \
                                self.src_table.c.service_request_number)\
                        .join(self.dup_table, self.src_table.c.line_num == self.dup_table.c.line_num))\
                    .where(or_(*and_args))\
                    .where(and_(self.dat_table.c.current_flag == True, 
                        or_(getattr(self.src_table.c, bk) != None, 
                            getattr(self.dat_table.c, bk) != None)))
            )
        conn = engine.connect()
        try:
            conn.execute(ins)
            return True
        except TypeError:
            # No changes found
            return False

    def _update_dat_current_flag(self):
        # Step Nine: Update data table with changed records

        # Need to figure out how to make the end_date more granular than a day. 
        # For datasets that update more frequently than every day, this will be
        # crucial so that we are updating the current_flag on the correct records.
        pk = getattr(self.dat_table.c, '%s_row_id' % self.dataset_name)
        update = self.dat_table.update()\
            .values(current_flag=False, end_date=datetime.now().strftime('%Y-%m-%d'))\
            .where(pk == self.chg_table.c.id)\
            .where(self.dat_table.c.current_flag == True)
        conn = engine.connect()
        conn.execute(update)
        return None

    def _update_master_current_flag(self):
        # Step Ten: Update master table with changed records

        # Need to figure out how to make the end_date more granular than a day. 
        # For datasets that update more frequently than every day, this will be
        # crucial so that we are updating the current_flag on the correct records.
        mt = MasterTable.__table__
        update = mt.update()\
            .values(current_flag=False, end_date=datetime.now().strftime('%Y-%m-%d'))\
            .where(mt.c.dataset_row_id == getattr(self.dat_table.c, '%s_row_id' % self.dataset_name))\
            .where(self.dat_table.c.current_flag == False)\
            .where(self.dat_table.c.end_date == datetime.now().strftime('%Y-%m-%d'))
        conn = engine.connect()
        conn.execute(update)
        return None

    def _update_meta(self, added=False):
        """ 
        Update the meta_master table with obs_from, obs_to, 
        updated_date, bbox, and (when appropriate) date_added
        """
        md = session.query(MetaTable)\
            .filter(MetaTable.source_url == self.source_url)\
            .first()
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
        if self.latitude and self.longitude:
            lat_col = getattr(self.dat_table.c, slugify(self.latitude))
            lon_col = getattr(self.dat_table.c, slugify(self.longitude))
            xmin, ymin, xmax, ymax = session.query(
                                         func.min(lat_col),
                                         func.min(lon_col),
                                         func.max(lat_col),
                                         func.max(lon_col))\
                                         .first()
        elif self.location:
            loc_col = getattr(self.dat_table.c, slugify(self.location))
            subq = session.query(
                cast(func.regexp_matches(loc_col, '\((.*),.*\)'), 
                    ARRAY(Float)).label('lat'), 
                cast(func.regexp_matches(loc_col, '\(.*,(.*)\)'), 
                    ARRAY(Float)).label('lon'))\
                .subquery()
            xmin, ymin, xmax, ymax = session.query(func.min(subq.c.lat), 
                                                   func.min(subq.c.lon), 
                                                   func.max(subq.c.lat), 
                                                   func.min(subq.c.lon))\
                                            .first()
            xmin, ymin, xmax, ymax = xmin[0], ymin[0], xmax[0], ymax[0]
        md.bbox = from_shape(box(xmin, ymin, xmax, ymax), srid=4326)
        session.add(md)
        session.commit()
