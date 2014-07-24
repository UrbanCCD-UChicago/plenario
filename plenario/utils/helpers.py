import requests
import re
import os
from datetime import datetime, date, time
from unicodedata import normalize
from plenario.database import task_session as session, task_engine as engine, \
    Base
from plenario.models import MetaTable, MasterTable
from urlparse import urlparse
from csvkit.unicsv import UnicodeCSVReader
from csvkit.typeinference import normalize_table
import gzip
from sqlalchemy import Boolean, Float, DateTime, Date, Time, String, Column, \
    Integer, Table, text, func, select
from sqlalchemy.dialects.postgresql import TIMESTAMP
from types import NoneType

DATA_DIR = os.environ['WOPR_DATA_DIR']

COL_TYPES = {
    bool: Boolean,
    int: Integer,
    float: Float, 
    datetime: TIMESTAMP, 
    date: Date,
    time: Time,
    NoneType: String,
    unicode: String
}

def download_csv(url, fname):
    r = requests.get(url, stream=True)
    fpath = '%s/%s_%s.csv.gz' % (DATA_DIR, fname, datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))
    with gzip.open(os.path.join(fpath), 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
                f.flush()
    return fpath

def initialize_table(source_url):
    # Step One: Make a table where the data will eventually live
    domain = urlparse(source_url).netloc
    fourbyfour = source_url.split('/')[-1]
    view_url = 'http://%s/api/views/%s' % (domain, fourbyfour)
    dl_url = '%s/rows.csv?accessType=DOWNLOAD' % view_url
    md = session.query(MetaTable).get(source_url)
    fpath = download_csv(dl_url, md.dataset_name)
    has_nulls = {}
    with gzip.open(fpath, 'rb') as f:
        reader = UnicodeCSVReader(f)
        header = reader.next()
        col_types,col_vals = normalize_table(reader)
        for idx, col in enumerate(col_vals):
            if None in col_vals:
                has_nulls[header[idx]] = True
            else:
                has_nulls[header[idx]] = False
    cols = [
        Column('start_date', TIMESTAMP, server_default=text('CURRENT_TIMESTAMP')),
        Column('end_date', TIMESTAMP, server_default=text('NULL')),
        Column('current_flag', Boolean, server_default=text('TRUE')),
        Column('dataset_row_id', Integer, primary_key=True),
    ]
    for col_name,d_type in zip(header, col_types):
        kwargs = {}
        if has_nulls[col_name]:
            kwargs['nullable'] = True
        cols.append(Column(slugify(col_name), COL_TYPES[d_type], **kwargs))
    table = Table('dat_%s' % md.dataset_name, Base.metadata, *cols)
    table.create(engine, checkfirst=True)
    return fpath

def insert_raw_data(fpath, meta):
    # Step Two: Insert data directly from CSV
    table = Table('dat_%s' % meta['dataset_name'], Base.metadata, 
                  autoload=True, autoload_with=engine, extend_existing=True)
    cols = []
    skip_cols = ['start_date', 'end_date', 'current_flag', 'dataset_row_id']
    for col in table.columns:
        kwargs = {}
        if col.name not in skip_cols:
            cols.append(Column(col.name, col.type, **kwargs))
    raw_table = Table('raw_%s' % meta['dataset_name'], Base.metadata, 
                      *cols, extend_existing=True)
    raw_table.drop(bind=engine, checkfirst=True)
    raw_table.append_column(Column('dup_row_id', Integer, primary_key=True))
    raw_table.create(bind=engine, checkfirst=True)
    names = [c.name for c in table.columns]
    copy_st = 'COPY raw_%s (' % meta['dataset_name']
    for idx, name in enumerate(names):
        if name not in skip_cols:
            if idx < len(names) - 1:
                copy_st += '%s, ' % name
            else:
                copy_st += '%s)' % name
    else:
        copy_st += "FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')"
    conn = engine.raw_connection()
    cursor = conn.cursor()
    with gzip.open(fpath, 'rb') as f:
        cursor.copy_expert(copy_st, f)
    conn.commit()

def dedupe_raw_data(meta):
    # Step Three: Make sure to remove duplicates based upon what the user 
    # said was the business key
    raw_table = Table('raw_%s' % meta['dataset_name'], Base.metadata, 
                  autoload=True, autoload_with=engine, extend_existing=True)
    dedupe_table = Table('dedupe_%s' % meta['dataset_name'], Base.metadata,
                        Column('dup_row_id', Integer, primary_key=True), 
                        extend_existing=True)
    dedupe_table.drop(bind=engine, checkfirst=True)
    dedupe_table.create(bind=engine)
    pk = slugify(meta['business_key'])
    ins = dedupe_table.insert()\
        .from_select(
            ['dup_row_id'],
            select([func.max(raw_table.c.dup_row_id)])\
            .group_by(getattr(raw_table.c, pk))
        )
    conn = engine.connect()
    conn.execute(ins)

def make_src_table(meta):
    # Step Four: Make a table with every unique record.
    dat_table = Table('dat_%s' % meta['dataset_name'], Base.metadata, 
                  autoload=True, autoload_with=engine, extend_existing=True)
    raw_table = Table('raw_%s' % meta['dataset_name'], Base.metadata, 
                  autoload=True, autoload_with=engine, extend_existing=True)
    dedupe_table = Table('dedupe_%s' % meta['dataset_name'], Base.metadata, 
                  autoload=True, autoload_with=engine, extend_existing=True)
    cols = []
    skip_cols = ['start_date', 'end_date', 'current_flag']
    for col in dat_table.columns:
        if col.name not in skip_cols:
            kwargs = {}
            if col.primary_key:
                kwargs['primary_key'] = True
            if col.server_default:
                kwargs['server_default'] = col.server_default
            cols.append(Column(col.name, col.type, **kwargs))
    src_table = Table('src_%s' % meta['dataset_name'], Base.metadata, 
                      *cols, extend_existing=True)
    src_table.drop(bind=engine, checkfirst=True)
    src_table.create(bind=engine)
    ins = src_table.insert()\
        .from_select(
            [c for c in src_table.columns.keys() if c != 'dataset_row_id'],
            select([c for c in raw_table.columns if c.name != 'dup_row_id'])\
                .where(raw_table.c.dup_row_id == dedupe_table.c.dup_row_id)
        )
    conn = engine.connect()
    conn.execute(ins)
    return None

def find_new_records(meta):
    # Step Five: Find the new records
    dat_table = Table('dat_%s' % meta['dataset_name'], Base.metadata, 
                  autoload=True, autoload_with=engine, extend_existing=True)
    src_table = Table('src_%s' % meta['dataset_name'], Base.metadata, 
                  autoload=True, autoload_with=engine, extend_existing=True)
    bk = slugify(meta['business_key'])
    new_table = Table('new_%s' % meta['dataset_name'], Base.metadata,
                      Column('id', Integer, primary_key=True),
                      extend_existing=True)
    new_table.drop(bind=engine, checkfirst=True)
    new_table.create(bind=engine)
    ins = new_table.insert()\
        .from_select(
            ['id'],
            select([src_table.c.dataset_row_id])\
                .select_from(src_table.join(dat_table, 
                    getattr(src_table.c, bk) == \
                        getattr(dat_table.c, bk), isouter=True))\
                .where(dat_table.c.dataset_row_id == None)
        )
    conn = engine.connect()
    try:
        conn.execute(ins)
    except TypeError:
        # No new records
        pass

def update_dat_table(meta):
    # Step Six: Update the dat table
    dat_table = Table('dat_%s' % meta['dataset_name'], Base.metadata, 
                  autoload=True, autoload_with=engine, extend_existing=True)
    src_table = Table('src_%s' % meta['dataset_name'], Base.metadata, 
                  autoload=True, autoload_with=engine, extend_existing=True)
    new_table = Table('new_%s' % meta['dataset_name'], Base.metadata, 
                  autoload=True, autoload_with=engine, extend_existing=True)
    skip_cols = ['end_date', 'current_flag']
    dat_cols = [c for c in dat_table.columns.keys() if c not in skip_cols]
    src_cols = [text("'%s' AS start_date" % datetime.now().isoformat())]
    src_cols.extend([c for c in src_table.columns if c.name not in skip_cols])
    ins = dat_table.insert()\
        .from_select(
            dat_cols,
            select(src_cols)\
                .select_from(src_table.join(new_table,
                    src_table.c.dataset_row_id == new_table.c.id))
        )
    conn = engine.connect()
    conn.execute(ins)

def update_master(meta):
    dat_table = Table('dat_%s' % meta['dataset_name'], Base.metadata, 
                  autoload=True, autoload_with=engine, extend_existing=True)
    new_table = Table('new_%s' % meta['dataset_name'], Base.metadata, 
                  autoload=True, autoload_with=engine, extend_existing=True)
    dat_cols = [
        dat_table.c.start_date,
        dat_table.c.end_date,
        dat_table.c.current_flag,
    ]
    dat_cols.append(getattr(dat_table.c, slugify(meta['location']))\
        .label('location'))
    dat_cols.append(getattr(dat_table.c, slugify(meta['latitude']))\
        .label('latitude'))
    dat_cols.append(getattr(dat_table.c, slugify(meta['longitude']))\
        .label('longitude'))
    dat_cols.append(getattr(dat_table.c, slugify(meta['observed_date']))\
        .label('obs_date'))
    dat_cols.append(text("NULL AS obs_ts"))
    dat_cols.append(text("NULL AS geotag1"))
    dat_cols.append(text("NULL AS geotag2"))
    dat_cols.append(text("NULL AS geotag3"))
    dat_cols.append(text("'%s' AS dataset_name" % meta['dataset_name']))
    dat_cols.append(dat_table.c.dataset_row_id)
    dat_cols.append(text(
        "ST_PointFromText('POINT(' || dat_%s.%s || ' ' || dat_%s.%s || ')', 4326) \
              as location_geom" % (
                  meta['dataset_name'], meta['longitude'], 
                  meta['dataset_name'], meta['latitude'],
              )))
    mt = MasterTable.__table__
    ins = mt.insert()\
        .from_select(
            [c for c in mt.columns.keys() if c != 'master_row_id'],
            select(dat_cols)\
                .select_from(dat_table.join(new_table, 
                    dat_table.c.dataset_row_id == new_table.c.id)
                )
        )
    conn = engine.connect()
    conn.execute(ins)

def get_socrata_data_info(view_url):
    errors = []
    status_code = None
    try:
        r = requests.get(view_url)
        status_code = r.status_code
    except requests.exceptions.InvalidURL:
        errors.append('Invalid URL')
    except requests.exceptions.ConnectionError:
        errors.append('URL can not be reached')
    try:
        resp = r.json()
    except AttributeError:
        errors.append('No Socrata views endpoint available for this dataset')
        resp = None
    if resp:
        columns = resp.get('columns')
        if columns:
            dataset_info = {
                'name': resp['name'],
                'description': resp.get('description'),
                'columns': [],
                'view_url': view_url
            }
            try:
                dataset_info['update_freq'] = \
                    resp['metadata']['custom_fields']['Metadata']['Update Frequency']
            except KeyError:
                dataset_info['update_freq'] = None
            for column in columns:
                d = {
                    'human_name': column['name'],
                    'machine_name': column['fieldName'],
                    'data_type': column['dataTypeName'],
                    'description': column.get('description', ''),
                    'width': column['width'],
                    'sample_values': [],
                    'smallest': '',
                    'largest': '',
                }
                if column.get('cachedContents'):
                    cached = column['cachedContents']
                    if cached.get('top'):
                        d['sample_values'] = \
                            [c['item'] for c in cached['top']][:5]
                    if cached.get('smallest'):
                        d['smallest'] = cached['smallest']
                    if cached.get('largest'):
                        d['largest'] = cached['largest']
                    if cached.get('null'):
                        if cached['null'] > 0:
                            d['null_values'] = True
                        else:
                            d['null_values'] = False
                dataset_info['columns'].append(d)
        else:
            errors.append('Views endpoint not structured as expected')
    return dataset_info, errors, status_code

def slugify(text, delim=u'_'):
    punct_re = re.compile(r'[\t !"#$%&\'()*\-/<=>?@\[\\\]^_`{|},.]+')
    result = []
    for word in punct_re.split(text.lower()):
        word = normalize('NFKD', word).encode('ascii', 'ignore')
        if word:
            result.append(word)
    return unicode(delim.join(result))

