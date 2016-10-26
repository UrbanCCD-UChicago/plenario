"""model_helpers: Just a collection of functions which perform common
interactions with the models."""

import json
from sqlalchemy.exc import ProgrammingError
from plenario.database import app_engine as engine
from plenario.models import MetaTable as Meta, ShapeMetadata as SMeta


def fetch_meta(identifier):
    """Grab the meta record of a dataset.

    :param identifier: (str) dataset name or hash
    :returns (RowProxy) a SQLAlchemy row object"""

    result = Meta.query.filter(Meta.dataset_name == identifier).first()
    if result is None:
        result = Meta.query.filter(Meta.source_url_hash == identifier).first()
    if result is None:
        result = SMeta.query.filter(SMeta.dataset_name == identifier).first()
    if result is None:
        raise ValueError(identifier + " does not exist in any meta table.")
    return result


def fetch_table(identifier):
    """Grab the reflected Table object of a dataset.

    :param identifier: (str) dataset name or source_url_hash
    :returns (Table) a SQLAlchemy table object"""

    if table_exists(identifier):
        meta = fetch_meta(identifier)
        try:
            return meta.point_table
        except AttributeError:
            return meta.shape_table
    else:
        raise ValueError(identifier + " table does not exist.")


def table_exists(table_name):
    """Make an inexpensive query to the database. It the table does not exist,
    the query will cause a ProgrammingError.

    :param table_name: (string) table name
    :returns (bool) true if the table exists, false otherwise"""

    try:
        engine.execute("select '{}'::regclass".format(table_name))
        return True
    except ProgrammingError:
        return False


def knn(pk, geom, lng, lat, table, k):
    """Execute a spatial query to select k nearest neighbors given some point.

    :param pk: (str) primary key column name
    :param geom: (str) geom column name
    :param lng: (float) longitude
    :param lat: (float) latitude
    :param table: (str) target table name
    :param k: (int) number of results to return
    :returns: (list) of nearest k neighbors"""

    # Convert lng-lat to geojson point
    point = "'" + json.dumps({
        "type": "Point",
        "coordinates": [lng, lat]
    }) + "'"

    # How many to limit the initial bounding box query to
    k_10 = k * 10

    # Based off snippet provided on pg 253 of PostGIS In Action (2nd Edition)
    query = """
    WITH bbox_results AS (
      SELECT
        {pk},
        {geom},
        (SELECT ST_SetSRID(ST_GeomFromGeoJSON({geojson}), 4326)) AS ref_geom
      FROM {table}
      ORDER BY {geom} <#> (SELECT ST_SetSRID(ST_GeomFromGeoJSON({geojson}), 4326))
      LIMIT {k_10}
    )

    SELECT
      {pk},
      RANK() OVER(ORDER BY ST_Distance({geom}, ref_geom)) AS act_r
    FROM bbox_results
    ORDER BY act_r
    LIMIT {k};
    """.format(pk=pk, geom=geom, geojson=point, table=table, k=k, k_10=k_10)

    return engine.execute(query).fetchall()
