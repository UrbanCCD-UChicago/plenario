"""model_helpers: Just a collection of functions which perform common
interactions with the models."""

from sqlalchemy.exc import ProgrammingError

from plenario.database import app_engine, session
from plenario.etl.point import PlenarioETL
from plenario.etl.shape import ShapeETL
from plenario.models import MetaTable, ShapeMetadata


def add_meta_if_not_exists(app, table_name, post_data, shape=False):
    """Add a meta record through the post endpoint. Meant for use
    with tests.

    :param table_name: (string)
    :param app: (FlaskApp) TestClient instance used to make requests
    :param post_data: (list) data that is used to make post request
    :param shape: (bool) designates which meta record to use"""

    if not meta_exists(table_name, shape):
        shape = 'true' if shape else 'false'
        app.post('/add?is_shapefile={}'.format(shape), data=post_data)


def meta_exists(table_name, shape=False):
    """Check if a meta record exists for the table name.

    :param table_name: (string)
    :param shape: (bool) designates which meta table to use
    :returns: (bool) True if a meta record exists, False if otherwise"""

    meta = 'meta_shape' if shape else 'meta_master'
    return app_engine.execute("select exists("
                              "select 1 from {} where dataset_name='{}'"
                              ")"
                              .format(meta, table_name)).scalar()


def drop_meta_if_exists(table_name, shape=False):
    """Fetch a meta record for the specified table name.

    :param table_name: (string)
    :param shape: (bool) designates which meta table to use"""

    if meta_exists(table_name, shape):
        meta = 'shape' if shape else 'master'
        app_engine.execute("delete from meta_{} where dataset_name = '{}'"
                           .format(meta, table_name))


def fetch_meta(dataset_name, shape=False):
    """Fetch a meta record for the specified table name.

    :param dataset_name: (string) table name
    :param shape: (bool) designates which meta table to use
    :returns: (Table) SQLAlchemy table object"""

    if shape:
        return ShapeMetadata.get_by_dataset_name(dataset_name)
    else:
        return MetaTable.get_by_dataset_name(dataset_name)


def fetch_table(dataset_name, shape=False):
    """Fetch a table instance for the specified table name.

    :param dataset_name: (string) table name
    :param shape: (bool) designates which meta table to use
    :returns: (Table) SQLAlchemy table object"""

    if shape:
        return ShapeMetadata.get_by_dataset_name(dataset_name).shape_table
    else:
        return MetaTable.get_by_dataset_name(dataset_name).point_table


def table_exists(table_name):
    """Make an inexpensive query to the database. It the table does not exist,
    the query will cause a ProgrammingError.

    :param table_name: (string) table name
    :returns: (bool) true if the table exists, false otherwise"""

    try:
        app_engine.execute("select '{}'::regclass".format(table_name))
        return True
    except ProgrammingError:
        return False


def drop_table_if_exists(table_name, shape=False):
    """Meant for use in test suites and nowhere else. Helps to maintain
    the self-contained nature of unit tests.

    :param table_name: (string) table name
    :param shape: (bool) designates which meta table to use."""

    if table_exists(table_name):
        if shape:
            table = fetch_table(table_name, shape=True)
            meta = fetch_meta(table_name, shape=True)
            meta.is_ingested = False
        else:
            table = fetch_table(table_name, shape=False)
        table.drop()
        session.commit()


def add_table_if_not_exists(table_name, shape=False, source=None):
    """Meant for use in test suites and nowhere else. Helps to maintain
    the self-contained nature of unit tests.

    :param table_name: (string) table name
    :param shape: (bool) designates which ETL handles the table
    :param source: (string) optional path to a local dataset file"""

    if not table_exists(table_name):
        if shape:
            meta = fetch_meta(table_name, shape)
            etl = ShapeETL(meta)
            etl.meta.is_ingested = False
            etl.add()
        else:
            meta = fetch_meta(table_name, shape)
            PlenarioETL(meta).add()


def fetch_pending_tables(model):
    """Used in views.py, fetch all records corresponding to tables pending
    administrator approval. These tables exist in the master tables, but their
    corresponding records have not been ingested.

    :param model: (class) ORM Class corresponding to a meta table
    :returns: (list) contains all records for which is_approved is false"""

    query = session.query(model).filter(model.approved_status is not True)
    return query.all()


# def fetch_table_etl_status(model):
#     """Used in views.py, fetch all records corresponding to tables that have
#     entered the ETL process. Used to report successful, ongoing, or failed
#     ETL tasks.
#
#     :param model: (class) ORM Class corresponding to a meta table
#     :returns: (list) contains all records for datasets and ETL status"""
#
#     query = session.query(model, ETLTask.status, ETLTask.error)
#     return query.all()
