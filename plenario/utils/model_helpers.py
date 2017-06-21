"""model_helpers: Just a collection of functions which perform common
interactions with the models."""

import json
from sqlalchemy.exc import ProgrammingError
from plenario.database import app_engine as engine
from plenario.models import MetaTable as Meta, ShapeMetadata as SMeta


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


def knn(lng, lat, k, network, sensors):
    """Execute a spatial query to select k nearest neighbors given some point.

    :param lng: (float) longitude
    :param lat: (float) latitude
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
        node,
        location,
        array_agg(sensor) AS sensors,
        (SELECT ST_SetSRID(ST_GeomFromGeoJSON({geojson}), 4326)) AS ref_geom
      FROM
        sensor__node_metadata JOIN sensor__sensor_to_node
        ON id=node
      WHERE
          sensor_network = '{network}'
      GROUP BY
        node,
        location
      ORDER BY
        location <#> (SELECT ST_SetSRID(ST_GeomFromGeoJSON ({geojson}), 4326))
      LIMIT {k_10}
    )

    SELECT
      node,
      RANK() OVER(ORDER BY ST_Distance(location, ref_geom)) AS act_r
    FROM bbox_results
    WHERE
      sensors && '{sensors}'::VARCHAR[]
    ORDER BY act_r
    LIMIT {k};
    """.format(
        geojson=point,
        network=network,
        k=k,
        k_10=k_10,
        sensors="{" + ",".join(sensors) + "}"
    )

    return engine.execute(query).fetchall()
