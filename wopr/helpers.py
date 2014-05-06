import requests
import os
from datetime import datetime
from sqlalchemy import Column, Integer, Table, func, select
from wopr.database import engine, Base
from wopr.models import crime_table
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

def load_raw_crime(fpath=None):
    # Step One: Load raw downloaded data
    if not fpath:
        fpath = download_crime()
    raw_crime_table = crime_table('raw_chicago_crimes_all', Base.metadata)
    raw_crime_table.drop(bind=engine, checkfirst=True)
    raw_crime_table.append_column(Column('dup_row_id', Integer, primary_key=True))
    raw_crime_table.create(bind=engine)
    conn = engine.raw_connection()
    cursor = conn.cursor()
    with gzip.open(fpath, 'rb') as f:
        cursor.copy_expert("COPY raw_chicago_crimes_all \
            (id, case_number, date, block, iucr, primary_type, \
            description, location_description, arrest, domestic, \
            beat, district, ward, community_area, fbi_code, \
            x_coordinate, y_coordinate, year, updated_on, \
            latitude, longitude, location) FROM STDIN WITH \
            (FORMAT CSV, HEADER true, DELIMITER ',')", f)
    conn.commit()
    return raw_crime_table

def dedupe_crime_table():
    # Step Two: Find dubplicate records by id
    raw_crime_table = Table('raw_chicago_crimes_all', Base.metadata, 
        autoload=True, autoload_with=engine, extend_existing=True)
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

def create_src_table():
    # Step Three: Create New table with unique ids
    raw_crime_table = Table('raw_chicago_crimes_all', Base.metadata, 
        autoload=True, autoload_with=engine, extend_existing=True)
    dedupe_crime_table = Table('dedup_chicago_crimes_all', Base.metadata, 
        autoload=True, autoload_with=engine, extend_existing=True)
    src_crime_table = crime_table('src_chicago_crimes_all', Base.metadata)
    src_crime_table.drop(bind=engine, checkfirst=True)
    src_crime_table.create(bind=engine)
    ins = src_crime_table.insert()\
        .from_select(
            src_crime_table.columns.keys(),
            select([raw_crime_table])\
                .where(raw_crime_table.c.dup_row_id == dedupe_crime_table.c.dup_row_id)
        )
    print ins
    conn = engine.connect()
    conn.execute(ins)
