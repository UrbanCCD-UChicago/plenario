import requests
import os
from datetime import datetime
from sqlalchemy import Column, Integer, Table, func, select, Boolean, \
    UniqueConstraint, text
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.dialects.postgresql import TIMESTAMP
from wopr.database import engine, Base
from wopr.models import crime_table, MasterTable
import gzip

CRIMES = 'https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD'
AWS_KEY = os.environ['AWS_ACCESS_KEY']
AWS_SECRET = os.environ['AWS_SECRET_KEY']
DATA_DIR = os.environ['WOPR_DATA_DIR']

class SocrataError(Exception): 
    def __init__(self, message):
        Exception.__init__(self, message)
        self.message = message

def download_crime():
    r = requests.get(CRIMES, stream=True)
    fpath = '%s/crime_%s.csv' % (DATA_DIR, datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))
    with gzip.open(os.path.join(fpath), 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
                f.flush()
    return fpath

def dat_crime():
    # Step Zero: Create dat_crime table
    try:
        src_crime_table = Table('src_chicago_crimes_all', Base.metadata, 
            autoload=True, autoload_with=engine, extend_existing=True)
    except NoSuchTableError:
        src_crime_table = src_crime()
    dat_crime_table = crime_table('dat_chicago_crimes_all', Base.metadata)
    dat_crime_table.append_column(Column('chicago_crimes_all_row_id', Integer, primary_key=True))
    dat_crime_table.append_column(Column('start_date', TIMESTAMP, default=datetime.now))
    dat_crime_table.append_column(Column('end_date', TIMESTAMP, default=None))
    dat_crime_table.append_column(Column('current_flag', Boolean, default=True))
    dat_crime_table.append_constraint(UniqueConstraint('id', 'start_date'))
    dat_crime_table.create(bind=engine)
    new_cols = ['start_date', 'end_date', 'current_flag', 'chicago_crimes_row_id']
    dat_ins = dat_crime_table.insert()\
        .from_select(
            [c.name for c in dat_crime_table.columns.keys() if c.name not in new_cols],
            select([c for c in src_crime_table.columns])
        )
    conn = engine.connect()
    conn.execute(dat_ins)
    return dat_crime_table

def raw_crime(fpath=None, tablename='raw_chicago_crimes_all'):
    # Step One: Load raw downloaded data
    if not fpath:
        fpath = download_crime()
    raw_crime_table = crime_table(tablename, Base.metadata)
    raw_crime_table.drop(bind=engine, checkfirst=True)
    raw_crime_table.append_column(Column('dup_row_id', Integer, primary_key=True))
    raw_crime_table.create(bind=engine)
    conn = engine.raw_connection()
    cursor = conn.cursor()
    with gzip.open(fpath, 'rb') as f:
        cursor.copy_expert("COPY %s \
            (id, case_number, date, block, iucr, primary_type, \
            description, location_description, arrest, domestic, \
            beat, district, ward, community_area, fbi_code, \
            x_coordinate, y_coordinate, year, updated_on, \
            latitude, longitude, location) FROM STDIN WITH \
            (FORMAT CSV, HEADER true, DELIMITER ',')" % tablename, f)
    conn.commit()
    return raw_crime_table

def dedupe_crime():
    # Step Two: Find dubplicate records by id
    try:
        raw_crime_table = Table('raw_chicago_crimes_all', Base.metadata, 
            autoload=True, autoload_with=engine, extend_existing=True)
    except NoSuchTableError:
        raw_crime_table = raw_crime(tablename='raw_chicago_crimes_all')
    dedupe_crime_table = Table('dedup_chicago_crimes_all', Base.metadata,
        Column('dup_row_id', Integer, primary_key=True),
        extend_existing=True)
    dedupe_crime_table.drop(bind=engine, checkfirst=True)
    dedupe_crime_table.create(bind=engine)
    ins = dedupe_crime_table.insert()\
        .from_select(
            ['dup_row_id'], 
            select([func.max(raw_crime_table.c.dup_row_id)])\
            .group_by(raw_crime_table.c.id)
        )
    conn = engine.connect()
    conn.execute(ins)
    return dedupe_crime_table

def src_crime():
    # Step Three: Create New table with unique ids
    try:
        raw_crime_table = Table('raw_chicago_crimes_all', Base.metadata, 
            autoload=True, autoload_with=engine, extend_existing=True)
    except NoSuchTableError:
        raw_crime_table = raw_crime()
    try:
        dedupe_crime_table = Table('dedup_chicago_crimes_all', Base.metadata, 
            autoload=True, autoload_with=engine, extend_existing=True)
    except NoSuchTableError:
        dedupe_crime_table = dedupe_crime()
    src_crime_table = crime_table('src_chicago_crimes_all', Base.metadata)
    src_crime_table.drop(bind=engine, checkfirst=True)
    src_crime_table.create(bind=engine)
    ins = src_crime_table.insert()\
        .from_select(
            src_crime_table.columns.keys(),
            select([c for c in raw_crime_table.columns if c.name != 'dup_row_id'])\
                .where(raw_crime_table.c.dup_row_id == dedupe_crime_table.c.dup_row_id)
        )
    conn = engine.connect()
    conn.execute(ins)
    return src_crime_table

def new_crime():
    # Step Four: Insert new crimes into dat table
    try:
        dat_crime_table = Table('dat_chicago_crimes_all', Base.metadata, 
            autoload=True, autoload_with=engine, extend_existing=True)
    except NoSuchTableError:
        # bust this out into a function
        dat_crime_table = dat_crime()
    try:
        src_crime_table = Table('src_chicago_crimes_all', Base.metadata, 
            autoload=True, autoload_with=engine, extend_existing=True)
    except NoSuchTableError:
        src_crime_table = src_crime()
    new_crime_table = Table('new_chicago_crimes_all', Base.metadata, 
        Column('id', Integer, primary_key=True),
        extend_existing=True)
    new_crime_table.drop(bind=engine, checkfirst=True)
    new_crime_table.create(bind=engine)
    ins = new_crime_table.insert()\
        .from_select(
            ['id'],
            select([src_crime_table.c.id])\
                .select_from(src_crime_table.join(dat_crime_table, 
                    src_crime_table.c.id == dat_crime_table.c.id, isouter=True))\
                .where(dat_crime_table.c.chicago_crimes_all_row_id != None)
        )
    conn = engine.connect()
    conn.execute(ins)
    return new_crime_table

def update_master():
    # Step Five: Update Master table
    try:
        dat_crime_table = Table('dat_chicago_crimes_all', Base.metadata, 
            autoload=True, autoload_with=engine, extend_existing=True)
    except NoSuchTableError:
        dat_crime_table = dat_crime()
    try:
        new_crime_table = Table('new_chicago_crimes_all', Base.metadata, 
            autoload=True, autoload_with=engine, extend_existing=True)
    except NoSuchTableError:
        new_crime_table = new_crime()
    col_names = ['start_date', 'end_date', 'current_flag', 'location', 'latitude', 'longitude']
    cols = [c for c in dat_crime_table.columns if c.name in col_names]
    cols.append(dat_crime_table.c.orig_date.label('obs_date'))
    cols.append(text("NULL AS obs_ts"))
    cols.append(text("'chicago_crimes_all' AS dataset_name"))
    cols.append(text("ST_PointFromText('POINT(' || dat_chicago_crimes_all.longitude || ' ' || dat_chicago_crimes_all.latitude || ')') as location_geom"))
    cols.append(dat_crime_table.c.chicago_crimes_all_row_id.label('dataset_row_id'))
    ins = MasterTable.insert()\
        .from_select(
            MasterTable.columns.keys(),
            select(cols)\
                .select_from(dat_crime_table.join(new_crime_table, 
                    dat_crime_table.c.id == new_crime_table.c.id))
        )
    print ins
    conn = engine.connect()
    conn.execute(ins)
