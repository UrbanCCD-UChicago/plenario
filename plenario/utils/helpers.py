import requests
import re
import os
from datetime import datetime, date, time
from unicodedata import normalize
from plenario.database import task_session as session, task_engine as engine, \
    Base
from plenario.models import MetaTable
from urlparse import urlparse
from csvkit.unicsv import UnicodeCSVReader
from csvkit.typeinference import normalize_table
import gzip
from sqlalchemy import Boolean, Float, DateTime, Date, Time, String, Column, \
    Integer, Table, text
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
    ]
    for col_name,d_type in zip(header, col_types):
        kwargs = {}
        if has_nulls[col_name]:
            kwargs['nullable'] = True
        if col_name == md.business_key:
            kwargs['primary_key'] = True
        cols.append(Column(slugify(col_name, delim=u"_"), COL_TYPES[d_type], **kwargs))
    table = Table('dat_%s' % md.dataset_name, Base.metadata, *cols)
    table.create(engine, checkfirst=True)
    return fpath

def insert_raw_data(fpath, meta):
    table = Table('dat_%s' % meta['dataset_name'], Base.metadata, 
        autoload=True, autoload_with=engine, extend_existing=True)
    cols = []
    for col in table.columns:
        kwargs = {}
        if meta['business_key'] == col.name:
            kwargs['primary_key'] = True
        if col.server_default:
            kwargs['server_default'] = col.server_default
        kwargs['nullable'] = col.nullable
        cols.append(Column(col.name, col.type, **kwargs))
    raw_table = Table('raw_%s' % meta['dataset_name'], Base.metadata, *cols)
    raw_table.create(bind=engine, checkfirst=True)
    names = [c.name for c in table.columns]
    skip_cols = ['start_date', 'end_date', 'current_flag']
    copy_st = 'COPY raw_%s (' % meta['dataset_name']
    for idx, name in enumerate(names):
        if name not in skip_cols:
            if idx < len(names):
                copy_st += '%s, ' % name
            else:
                copy_st += '%s)' % name
    else:
        copy_st += "FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')"
    print copy_st
   #conn = engine.raw_connection()
   #cursor = conn.cursor()
   #with gzip.open(fpath, 'rb') as f:
   #    

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

def slugify(text, delim=u'-'):
    punct_re = re.compile(r'[\t !"#$%&\'()*\-/<=>?@\[\\\]^_`{|},.]+')
    result = []
    for word in punct_re.split(text.lower()):
        word = normalize('NFKD', word).encode('ascii', 'ignore')
        if word:
            result.append(word)
    return unicode(delim.join(result))

