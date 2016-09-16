"""model_helpers: Just a collection of functions which perform common
interactions with the models."""

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


def knn(pk, geom, pid, table, k):
    """Execute a spatial query to select k nearest neighbors given some point.

    :param pk: (str) primary key column name
    :param geom: (str) geom column name
    :param pid: (str) target point
    :param table: (str) target table name
    :param k: (int) number of results to return
    :returns: (list) of nearest k neighbors"""

    # Based off snippet provided on pg 253 of PostGIS In Action (2nd Edition)
    query = """
        with bounding_box_filtered as (
            select
                id,
                location,
                (select location from sensor__node_metadata where id = 'NODE_DEV_1') as ref_geom
            from sensor__node_metadata
            order by location <#> (select location from sensor__node_metadata as l where id = 'NODE_DEV_1')
            limit 100
        )

        select
        id, rank() over(order by ST_Distance(location, ref_geom)) as act_rank
        from bounding_box_filtered
        order by act_rank
        limit {k}
    """

    return engine.execute(q_knn).fetchall()
