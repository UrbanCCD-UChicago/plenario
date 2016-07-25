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
        raise ValueError(identifier + " ")


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
