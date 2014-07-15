import os
from celery import Task, Celery, chain
from datetime import datetime, timedelta
from wopr.database import task_engine as engine, Base
from wopr.models import crime_table, MasterTable, sf_crime_table, shp2table
from wopr.helpers import download_crime
from wopr import make_celery
from datetime import datetime, date
from sqlalchemy import Column, Integer, Table, func, select, Boolean, \
    UniqueConstraint, text, and_, or_
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.exc import NoSuchTableError
from geoalchemy2 import Geometry
import gzip
from raven.handlers.logging import SentryHandler
from raven.conf import setup_logging
from zipfile import ZipFile
import fiona
from shapely.geometry import shape, Polygon, MultiPolygon
import json

#handler = SentryHandler(os.environ['CELERY_SENTRY_URL'])
#setup_logging(handler)

celery_app = make_celery()

@celery_app.task
def update_crime(fpath=None):
    raw_crime(fpath=fpath)
    dedupe_crime()
    src_crime()
    new = new_crime()
    if new is not None:
        update_dat_crimes()
        update_master()
        chg_crime()
        update_crime_current_flag()
        update_master_current_flag()
    cleanup_temp_tables()
    return None

@celery_app.task
def cleanup_temp_tables():
    tables = ['new', 'src', 'raw', 'chg', 'dedup']
    for table in tables:
        try:
            t = Table('%s_chicago_crimes_all' % table, Base.metadata, 
                autoload=True, autoload_with=engine, extend_existing=True)
            t.drop(bind=engine, checkfirst=True)
        except NoSuchTableError:
            pass
    return 'Temp tables dropped'

@celery_app.task
def dat_crime(fpath=None):
    # Step Zero: Create dat_crime table
    raw_crime(fpath=fpath)
    dedupe_crime()
    src_crime()
    src_crime_table = Table('src_chicago_crimes_all', Base.metadata, 
        autoload=True, autoload_with=engine, extend_existing=True)
    dat_crime_table = crime_table('dat_chicago_crimes_all', Base.metadata)
    dat_crime_table.append_column(Column('chicago_crimes_all_row_id', Integer, primary_key=True))
    dat_crime_table.append_column(Column('start_date', TIMESTAMP, server_default=text('CURRENT_TIMESTAMP')))
    dat_crime_table.append_column(Column('end_date', TIMESTAMP, server_default=text('NULL')))
    dat_crime_table.append_column(Column('current_flag', Boolean, server_default=text('TRUE')))
    dat_crime_table.append_constraint(UniqueConstraint('id', 'start_date'))
    dat_crime_table.create(bind=engine, checkfirst=True)
    new_cols = ['start_date', 'end_date', 'current_flag', 'chicago_crimes_all_row_id']
    dat_ins = dat_crime_table.insert()\
        .from_select(
            [c for c in dat_crime_table.columns.keys() if c not in new_cols],
            select([c for c in src_crime_table.columns])
        )
    conn = engine.contextual_connect()
    res = conn.execute(dat_ins)
    cols = crime_master_cols(dat_crime_table)
    master_ins = MasterTable.insert()\
        .from_select(
            [c for c in MasterTable.columns.keys() if c != 'master_row_id'],
            select(cols)\
                .select_from(dat_crime_table)
        )
    conn = engine.contextual_connect()
    res = conn.execute(master_ins)
    cleanup_temp_tables()
    return 'DAT crime created'

def sf_dat_crime(fpath=None, crime_type='violent'):
    #raw_crime = sf_raw_crime(fpath=fpath)
    # Assume for now there's no duplicate in the raw data, which means we don't
    # - dedupe_crime()
    # - and don't create src_crime()
    raw_crime_table = Table('raw_sf_crimes_all', Base.metadata,
        autoload=True, autoload_with=engine, extend_existing=True)
    if crime_type == 'violent':
        categories = ['ASSAULT', 'ROBBERY', 'SEX OFFENSES, FORCIBLE']
    elif crime_type == 'property':
        categories = ['LARCENY/THEFT', 'VEHICLE THEFT', 'BURGLARY', 'STOLEN PROPERTY',\
                      'ARSON', 'VANDALISM']
    # Create table "dat_sf_crimes_all", that contains additional fields needed
    # by Plenario, in addition to the raw data
    dat_crime_table = sf_crime_table('dat_sf_crimes_{0}'.format(crime_type), Base.metadata)
    dat_crime_table.append_column(
        Column( 'sf_crimes_all_row_id', Integer,    primary_key=True                         ) )
    dat_crime_table.append_column(
        Column( 'start_date',           TIMESTAMP,  server_default=text('CURRENT_TIMESTAMP') ) )
    dat_crime_table.append_column(
        Column( 'end_date',             TIMESTAMP,  server_default=text('NULL')              ) )
    dat_crime_table.append_column(
        Column( 'current_flag',         Boolean,    server_default=text('TRUE')              ) )
    # Constrain (id, start_date) to be unique (?)
    # dat_crime_table.append_constraint(UniqueConstraint('id', 'start_date'))
    dat_crime_table.create(bind=engine, checkfirst=True)
    new_cols = ['start_date', 'end_date', 'current_flag', 'sf_crimes_all_row_id']
    # Insert data from raw_crime_table (to be src_crime_table when we'll check
    # for duplicates)
    dat_ins = dat_crime_table.insert()\
        .from_select(
            [c for c in dat_crime_table.columns.keys() if c not in new_cols],
            select([c for c in raw_crime_table.columns if c.name != 'dup_row_id'])\
                .where(raw_crime_table.c.category.in_(categories))
        )
    conn = engine.contextual_connect()
    res = conn.execute(dat_ins)
    cols = sf_crime_master_cols(dat_crime_table, crime_type=crime_type)
    master_ins = MasterTable.insert()\
        .from_select(
            [c for c in MasterTable.columns.keys() if c != 'master_row_id'],
            select(cols).select_from(dat_crime_table)
        )
    conn = engine.contextual_connect()
    res = conn.execute(master_ins)
    return 'DAT crime created'


@celery_app.task
def raw_crime(fpath=None, tablename='raw_chicago_crimes_all'):
    # Step One: Load raw downloaded data
    if not fpath:
        fpath = download_crime()
    print 'Crime file downloaded\n\n'
    raw_crime_table = crime_table(tablename, Base.metadata)
    raw_crime_table.drop(bind=engine, checkfirst=True)
    raw_crime_table.append_column(Column('dup_row_id', Integer, primary_key=True))
    raw_crime_table.create(bind=engine)
    conn = engine.raw_connection()
    cursor = conn.cursor()
    with gzip.open(fpath, 'rb') as f:
        cursor.copy_expert("COPY %s \
            (id, case_number, orig_date, block, iucr, primary_type, \
            description, location_description, arrest, domestic, \
            beat, district, ward, community_area, fbi_code, \
            x_coordinate, y_coordinate, year, updated_on, \
            latitude, longitude, location) FROM STDIN WITH \
            (FORMAT CSV, HEADER true, DELIMITER ',')" % tablename, f)
    conn.commit()
    return 'Raw Crime data inserted'

def sf_raw_crime(fpath=None, tablename='raw_sf_crimes_all'):
    if not fpath:
        fpath = download_crime()
    print 'SF crime data downloaded\n\n'
    raw_crime_table = sf_crime_table(tablename, Base.metadata)
    raw_crime_table.drop(bind=engine, checkfirst=True)
    raw_crime_table.append_column(Column('dup_row_id', Integer, primary_key=True))
    raw_crime_table.create(bind=engine)
    conn = engine.raw_connection()
    cursor = conn.cursor()
    zf = ZipFile(fpath)
    # SF crime data has one file for each year...
    for fn in zf.namelist():
        with zf.open(fn, 'r') as f:
            cursor.copy_expert("COPY %s \
                (id, category, description, day_of_week, date, time, pd_district, \
                 resolution, location_str, longitude, latitude) FROM STDIN WITH \
                (FORMAT CSV, HEADER true, DELIMITER ',')" % tablename, f)
        print '{0} imported'.format(fn)
    conn.commit()
    zf.close()
    return 'Raw Crime data inserted'


@celery_app.task
def dedupe_crime():
    # Step Two: Find duplicate records by ID
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
    conn = engine.contextual_connect()
    res = conn.execute(ins)
    return 'Raw crime deduplicated'

@celery_app.task
def src_crime():
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
            select([c for c in raw_crime_table.columns if c.name != 'dup_row_id'])\
                .where(raw_crime_table.c.dup_row_id == dedupe_crime_table.c.dup_row_id)
        )
    conn = engine.contextual_connect()
    conn.execute(ins)
    return 'Source table created'

@celery_app.task
def new_crime():
    # Step Four: Find New Crimes
    dat_crime_table = Table('dat_chicago_crimes_all', Base.metadata, 
        autoload=True, autoload_with=engine, extend_existing=True)
    src_crime_table = Table('src_chicago_crimes_all', Base.metadata, 
        autoload=True, autoload_with=engine, extend_existing=True)
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
                .where(dat_crime_table.c.chicago_crimes_all_row_id == None)
        )
    conn = engine.contextual_connect()
    try:
        conn.execute(ins)
        return 'New records found'
    except TypeError:
        # No new records
        return None

@celery_app.task
def update_dat_crimes():
    # Step Five: Update Main Crime table
    dat_crime_table = Table('dat_chicago_crimes_all', Base.metadata, 
        autoload=True, autoload_with=engine, extend_existing=True)
    src_crime_table = Table('src_chicago_crimes_all', Base.metadata, 
        autoload=True, autoload_with=engine, extend_existing=True)
    try:
        new_crime_table = Table('new_chicago_crimes_all', Base.metadata, 
            autoload=True, autoload_with=engine, extend_existing=True)
    except NoSuchTableError:
        return None
    excluded_cols = ['end_date', 'current_flag', 'chicago_crimes_all_row_id']
    dat_cols = [c for c in dat_crime_table.columns.keys() if c not in excluded_cols]
    excluded_cols.append('start_date')
    src_cols = [c for c in src_crime_table.columns if c.name not in excluded_cols]
    src_cols.append(text("'%s' AS start_date" % datetime.now().strftime('%Y-%m-%d')))
    ins = dat_crime_table.insert()\
        .from_select(
            dat_cols,
            select(src_cols)\
                .select_from(src_crime_table.join(new_crime_table,
                    src_crime_table.c.id == new_crime_table.c.id))
        )
    conn = engine.contextual_connect()
    conn.execute(ins)
    return 'Crime Table updated'

def crime_master_cols(dat_crime_table):
    col_names = ['start_date', 'end_date', 'current_flag', 'location', 'latitude', 'longitude']
    cols = [
        dat_crime_table.c.start_date,
        dat_crime_table.c.end_date,
        dat_crime_table.c.current_flag,
        dat_crime_table.c.location,
        dat_crime_table.c.latitude, 
        dat_crime_table.c.longitude,
    ]
    cols.append(dat_crime_table.c.orig_date.label('obs_date'))
    cols.append(text("NULL AS obs_ts"))
    cols.append(text("NULL AS geotag1"))
    cols.append(text("NULL AS geotag2"))
    cols.append(text("NULL AS geotag3"))
    cols.append(text("'chicago_crimes_all' AS dataset_name"))
    cols.append(dat_crime_table.c.chicago_crimes_all_row_id.label('dataset_row_id'))
    cols.append(text("ST_PointFromText('POINT(' || dat_chicago_crimes_all.longitude || ' ' || dat_chicago_crimes_all.latitude || ')', 4326) as location_geom"))
    return cols

def sf_crime_master_cols(dat_crime_table, crime_type='violent'):
    col_names = ['start_date', 'end_date', 'current_flag', 'latitude', 'longitude']
    cols = [
        dat_crime_table.c.start_date,
        dat_crime_table.c.end_date,
        dat_crime_table.c.current_flag,
        dat_crime_table.c.latitude,
        dat_crime_table.c.longitude
    ]
    cols.append(dat_crime_table.c.date.label('obs_date'))
    cols.append(text("NULL AS obs_ts"))
    cols.append(text("NULL AS geotag1"))
    cols.append(text("NULL AS geotag2"))
    cols.append(text("NULL AS geotag3"))
    cols.append(text("'sf_crimes_{0}' AS dataset_name".format(crime_type)))
    cols.append(dat_crime_table.c.sf_crimes_all_row_id.label('dataset_row_id'))
    #cols.append(text("ST_PointFromText('POINT(' || dat_sf_crimes_all.longitude || ' ' || dat_sf_crimes_all.latitude || ')') as location"))
    cols.append(text("ST_PointFromText('POINT(' || dat_sf_crimes_{0}.longitude || ' ' || dat_sf_crimes_{0}.latitude || ')', 4326) as location_geom".format(crime_type)))
    return cols

@celery_app.task
def update_master():
    # Step Six: Update Master table
    dat_crime_table = Table('dat_chicago_crimes_all', Base.metadata, 
        autoload=True, autoload_with=engine, extend_existing=True)
    try:
        new_crime_table = Table('new_chicago_crimes_all', Base.metadata, 
            autoload=True, autoload_with=engine, extend_existing=True)
    except NoSuchTableError:
        return None
    cols = crime_master_cols(dat_crime_table)
    ins = MasterTable.insert()\
        .from_select(
            [c for c in MasterTable.columns.keys() if c != 'master_row_id'],
            select(cols)\
                .select_from(dat_crime_table.join(new_crime_table, 
                    dat_crime_table.c.id == new_crime_table.c.id))
        )
    conn = engine.contextual_connect()
    conn.execute(ins)
    return 'Master updated'

@celery_app.task
def chg_crime():
    # Step Seven: Find updates
    dat_crime_table = Table('dat_chicago_crimes_all', Base.metadata, 
        autoload=True, autoload_with=engine, extend_existing=True)
    src_crime_table = Table('src_chicago_crimes_all', Base.metadata, 
        autoload=True, autoload_with=engine, extend_existing=True)
    chg_crime_table = Table('chg_chicago_crimes_all', Base.metadata, 
        Column('id', Integer, primary_key=True),
        extend_existing=True)
    chg_crime_table.drop(bind=engine, checkfirst=True)
    chg_crime_table.create(bind=engine)
    src_cols = [c for c in src_crime_table.columns if c.name not in ['id', 'start_date', 'end_date']]
    dat_cols = [c for c in dat_crime_table.columns if c.name not in ['id', 'start_date', 'end_date']]
    and_args = []
    for s, d in zip(src_cols, dat_cols):
        ors = or_(s != None, d != None)
        ands = and_(ors, s != d)
        and_args.append(ands)
    ins = chg_crime_table.insert()\
          .from_select(
              ['id'],
              select([src_crime_table.c.id])\
                  .select_from(src_crime_table.join(dat_crime_table,
                      src_crime_table.c.id == dat_crime_table.c.id))\
                  .where(or_(
                          and_(dat_crime_table.c.current_flag == True, 
                                and_(or_(src_crime_table.c.id != None, dat_crime_table.c.id != None), 
                                src_crime_table.c.id != dat_crime_table.c.id)),
                          *and_args))
          )
    conn = engine.contextual_connect()
    conn.execute(ins)
    return 'Changes found'

@celery_app.task
def update_crime_current_flag():
    # Step Seven: Update end_date and current_flag in crime table
    dat_crime_table = Table('dat_chicago_crimes_all', Base.metadata, 
        autoload=True, autoload_with=engine, extend_existing=True)
    chg_crime_table = Table('chg_chicago_crimes_all', Base.metadata, 
        autoload=True, autoload_with=engine, extend_existing=True)
    update = dat_crime_table.update()\
        .values(current_flag=False, end_date=datetime.now().strftime('%Y-%m-%d'))\
        .where(dat_crime_table.c.id==chg_crime_table.c.id)\
        .where(dat_crime_table.c.current_flag == True)
    conn = engine.contextual_connect()
    conn.execute(update)
    return 'Crime table current flag updated'

@celery_app.task
def update_master_current_flag():
    # Step Eight: Update end_date and current_flag in master table
    dat_crime_table = Table('dat_chicago_crimes_all', Base.metadata, 
        autoload=True, autoload_with=engine, extend_existing=True)
    update = MasterTable.update()\
        .values(current_flag=False, end_date=datetime.now().strftime('%Y-%m-%d'))\
        .where(MasterTable.c.dataset_row_id == dat_crime_table.c.chicago_crimes_all_row_id)\
        .where(dat_crime_table.c.current_flag==False)\
        .where(dat_crime_table.c.end_date==date.today())
    conn = engine.contextual_connect()
    conn.execute(update)
    return 'Master table current flag updated'

def import_shapefile(fpath, name, force_multipoly=False):
    # Open the shapefile with fiona
    with fiona.open('/', vfs='zip://{0}'.format(fpath)) as shp:
        shp_table = shp2table(name, Base.metadata, shp.schema,
            force_multipoly=force_multipoly)
        shp_table.drop(bind=engine, checkfirst=True)
        shp_table.append_column(Column('row_id', Integer, primary_key=True))
        shp_table.create(bind=engine)
        features = []
        count = 0
        for r in shp:
            if not force_multipoly and r['geometry']['type'] == 'MultiPolygon':
                return import_shapefile(fpath, name, force_multipoly=True)
            row_dict = dict(r['properties'])
            geom = shape(json.loads(str(r['geometry']).replace('\'', '"')\
                .replace('(', '[').replace(')', ']')))
            if force_multipoly and r['geometry']['type'] != 'MultiPolygon':
                geom = MultiPolygon([geom])
            row_dict['geom'] = geom.wkt
            features.append(row_dict)
            #if count > 9: break
            count += 1
    ins = shp_table.insert(features)
    conn = engine.contextual_connect()
    conn.execute(ins)
    return 'Table {0} created from shapefile'.format(name)
