import requests
import os
import re
from unicodedata import normalize
from datetime import datetime, date
from sqlalchemy import Column, Integer, Table, func, select, Boolean, \
    Date, DateTime, UniqueConstraint, text, and_, or_
from sqlalchemy.dialects.postgresql import TIMESTAMP
from plenario.database import task_engine as engine, Base
import gzip
from zipfile import ZipFile
import fiona
from shapely.geometry import shape, Polygon, MultiPolygon
import json
import pyproj

def transform_proj(geom, source, target=4326):
    """Transform a geometry's projection.

    Keyword arguments:
    geom -- a (nested) list of points (i.e. geojson coordinates)
    source/target -- integer ESPG codes, or Proj4 strings
    """
    s_str = '+init=EPSG:{0}'.format(source) if type(source)==int else source
    t_str = '+init=EPSG:{0}'.format(target) if type(target)==int else target
    ps = pyproj.Proj(s_str, preserve_units=True)
    pt = pyproj.Proj(t_str, preserve_units=True)
    # This function works as a depth-first search, recursively calling itself until a
    # point is found, and converted (base case)
    if type(geom[0]) == list:
        res = []
        for r in geom:
            res.append(transform_proj(r, source, target))
        return res
    else: # geom must be a point
        res = pyproj.transform(ps, pt, geom[0], geom[1])
        return list(res)
    
def import_shapefile(fpath, name, force_multipoly=False, proj=4326):
    """Import a shapefile into the PostGIS database

    Keyword arguments:
    fpath -- path to a zipfile to be extracted
    name -- name given to the newly created table
    force_multipoly -- enforce that the gemoetries are multipolygons
    proj -- source projection spec (EPSG code or Proj$ string)
    """
    # Open the shapefile with fiona.
    with fiona.open('/', vfs='zip://{0}'.format(fpath)) as shp:
        shp_table = shp2table(name, Base.metadata, shp.schema,
            force_multipoly=force_multipoly)
        shp_table.drop(bind=engine, checkfirst=True)
        shp_table.append_column(Column('row_id', Integer, primary_key=True))
        shp_table.create(bind=engine)
        features = []
        count = 0
        for r in shp:
            # ESRI shapefile don't contemplate multipolygons, i.e. the geometry
            # type is polygon even if multipolygons are contained.
            # If and when the 1st multipoly is encountered, the table is
            # re-initialized.
            if not force_multipoly and r['geometry']['type'] == 'MultiPolygon':
                return import_shapefile(fpath, name, force_multipoly=True, proj=proj)
            row_dict = dict((k.lower(), v) for k, v in r['properties'].iteritems())
            # GeoJSON intermediate representation
            geom_json = json.loads(str(r['geometry']).replace('\'', '"')\
                                   .replace('(', '[').replace(')', ']'))
            # If the projection is not long/lat (WGS84 - EPGS:4326), transform.
            if proj != 4326:
                geom_json['coordinates'] = transform_proj(geom_json['coordinates'], proj, 4326)
            # Shapely intermediate representation, used to obtained the WKT
            geom = shape(geom_json)
            if force_multipoly and r['geometry']['type'] != 'MultiPolygon':
                geom = MultiPolygon([geom])
            row_dict['geom'] = 'SRID=4326;{0}'.format(geom.wkt)
            features.append(row_dict)
            count += 1
            # Buffer DB writes
            if not count % 1000:
                ins = shp_table.insert(features)
                conn = engine.contextual_connect()
                conn.execute(ins)
                features = []
    ins = shp_table.insert(features)
    conn = engine.contextual_connect()
    conn.execute(ins)
    return 'Table {0} created from shapefile'.format(name)
