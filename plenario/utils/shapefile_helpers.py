import requests
import os
import re
from unicodedata import normalize
from datetime import datetime, date
from sqlalchemy import Column, Integer, Table, func, select, Boolean, \
    Date, DateTime, UniqueConstraint, text, and_, or_, Float, String
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.exc import NoSuchTableError
from geoalchemy2 import Geometry
from plenario.database import task_engine as engine, Base
from plenario.models import shp2table
from plenario.settings import AWS_ACCESS_KEY, AWS_SECRET_KEY, S3_BUCKET
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import gzip
import zipfile
import shapefile
from shapely.geometry import shape, Polygon, MultiPolygon, asShape
import json
# import pyproj
from cStringIO import StringIO
from itertools import izip_longest
from urlparse import urlparse

ENDPOINT = 'http://www2.census.gov/geo/tiger/TIGER2010'

TYPE_MAP = {
    'C': String,
    'N': Float,
    'L': Float,
    'D': TIMESTAMP,
}

class PlenarioShapeETL(object):
    """ 
    Downloads, transforms and loads shapefiles. 
    Expects input to be a zipfile containing the various
    component files of the ESRI Shapefile format. When initialized,
    you need to give it a dict with these keys and values:

        dataset_name:  Machine version of the dataset name.
                       This is used to name the primary key field of the
                       data table for the dataset as well as the table
                       itself.  Should be lowercase with words seperated
                       by underscores. Truncated to the first 50
                       characters.

        source_url:    This is used to download the raw data.

    """
    def __init__(self, meta):
        for k,v in meta.items():
            setattr(self, k, v)
        fname = urlparse(self.source_url).path.split('/')[-1]
        s3_path = '%s/%s_%s.zip' % \
            (self.dataset_name, 
                fname, datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))
        s3conn = S3Connection(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        bucket = s3conn.get_bucket(S3_BUCKET)
        self.s3_key = Key(bucket)
        self.s3_key.key = s3_path

    def add(self):
        self._download()
        self._load_shapefile()
        self._get_or_create_table()
        self._insert_data()

    def _download(self):
        """ 
        Download and cache file on S3
        """
        r = requests.get(self.source_url, stream=True)
        s = StringIO()
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                s.write(chunk)
                s.flush()
        s.seek(0)
        self.s3_key.set_contents_from_file(s)
        self.s3_key.make_public()
    
    def _load_shapefile(self):
        content = StringIO()
        file_contents = self.s3_key.get_contents_to_file(content)
        content.seek(0)
        shp = StringIO()
        dbf = StringIO()
        shx = StringIO()
        with zipfile.ZipFile(content, 'r') as f:
            for name in f.namelist():
                if name.endswith('.shp'):
                    shp.write(f.read(name))
                if name.endswith('.shx'):
                    shx.write(f.read(name))
                if name.endswith('.dbf'):
                    dbf.write(f.read(name))
        shp.seek(0)
        shx.seek(0)
        dbf.seek(0)
        shape_reader = shapefile.Reader(shp=shp, dbf=dbf, shx=shx)
        self.fields = shape_reader.fields[1:]
        self.records = shape_reader.shapeRecords()

    def _get_or_create_table(self):
        try:
            self.table = Table(self.dataset_name, Base.metadata, 
                autoload=True, autoload_with=engine, extend_existing=True)
            self.multipolygon = False
            if self.table.columns['geom'].type.geometry_type == 'MULTIPOLYGON':
                self.multipolygon = True
        except NoSuchTableError:
            columns = []
            for field in self.fields:
                fname, d_type, f_len, d_len = field
                col_type = TYPE_MAP[d_type]
                kwargs = {}
                if d_type == 'C':
                    col_type = col_type(f_len)
                elif d_type == 'N':
                    col_type = col_type(d_len)
                if fname.lower() == self.business_key:
                    kwargs['primary_key'] = True
                columns.append(Column(fname.lower(), col_type, **kwargs))
            self.multipolygon = False
            for record in self.records:
                geo_type = record.shape.__geo_interface__['type']
                if 'multi' in geo_type.lower():
                    self.multipolygon = True
            geo_type = 'POLYGON'
            if self.multipolygon:
                geo_type = 'MULTIPOLYGON'
            columns.append(Column('geom', Geometry(geo_type)))
            self.table = Table(self.dataset_name, Base.metadata, *columns)
            self.table.create(engine, checkfirst=True)

    def _insert_data(self):
        fields = self.table.columns.keys()
        values = []
        for record in self.records:
            d = {}
            for k,v in zip(fields, record.record):
                d[k] = v
            geom = asShape(record.shape.__geo_interface__)
            if self.multipolygon and geom.geom_type.lower() == 'polygon':
                geom = MultiPolygon([geom])
            d['geom'] = 'SRID=4326;%s' % geom.wkt
            values.append(d)
        ins = self.table.insert()
        conn = engine.contextual_connect()
        conn.execute(ins, values)
        conn.close()
