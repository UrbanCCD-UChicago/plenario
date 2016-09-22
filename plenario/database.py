import os
from sqlalchemy import create_engine, and_, text, func
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base

from plenario.settings import DATABASE_CONN, REDSHIFT_CONN

if os.environ.get('WORKER'):
    app_engine = create_engine(DATABASE_CONN, convert_unicode=True, pool_size=8, max_overflow=8, pool_timeout=60, echo=False)
else:
    app_engine = create_engine(DATABASE_CONN, convert_unicode=True, pool_size=4, max_overflow=4, pool_timeout=60, echo=False)

session = scoped_session(sessionmaker(bind=app_engine,
                                      autocommit=False,
                                      autoflush=False, expire_on_commit=False))
Base = declarative_base(bind=app_engine)
Base.query = session.query_property()


redshift_engine = create_engine(REDSHIFT_CONN, convert_unicode=True, echo=False)

redshift_session = scoped_session(sessionmaker(bind=redshift_engine,
                                      autocommit=False,
                                      autoflush=False, expire_on_commit=False))
redshift_Base = declarative_base(bind=redshift_engine)
redshift_Base.query = redshift_session.query_property()


# Efficient query of large datasets (for use in DataDump)
# Referenced from https://bitbucket.org/zzzeek/sqlalchemy/wiki/UsageRecipes/WindowedRangeQuery
def column_windows(session, column, windowsize):
    """Return a series of WHERE clauses against
    a given column that break it into windows.

    Result is an iterable of tuples, consisting of
    ((start, end), whereclause), where (start, end) are the ids.

    Requires a database that supports window functions,
    i.e. Postgresql, SQL Server, Oracle.

    Enhance this yourself !  Add a "where" argument
    so that windows of just a subset of rows can
    be computed.

    """
    def int_for_range(start_id, end_id):
        if end_id:
            return and_(
                column>=start_id,
                column<end_id
            )
        else:
            return column>=start_id

    q = session.query(
                    column,
                    func.row_number().over(order_by=column).label('rownum')
                ).from_self(column)
    if windowsize > 1:
        q = q.filter(text("rownum %% %d=1" % windowsize))

    # Changed this line from the original implementation
    # For some reason our query results in several extra Nones
    # at the end. Inserted a condition here to remove those,
    # and it seems to work.
    intervals = [id for id, in q if id is not None]

    while intervals:
        start = intervals.pop(0)
        if intervals:
            end = intervals[0]
        else:
            end = None
        yield int_for_range(start, end)


def windowed_query(q, column, windowsize):
    """"Break a Query into windows on a given column."""

    for whereclause in column_windows(q.session, column, windowsize):
        for row in q.filter(whereclause).order_by(column):
            yield row


# Fast Counting of large datasets (for use with DataDump).
# Referenced from https://gist.github.com/hest/8798884
def fast_count(q):
    count_q = q.statement.with_only_columns([func.count()]).order_by(None)
    count = q.session.execute(count_q).scalar()
    return count
# Redshift connection setup

