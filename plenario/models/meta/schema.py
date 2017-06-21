import numpy
import pandas

from sqlalchemy import Column

from plenario.utils.helpers import slugify

from .types import numpy_sqlalchemy_type_map


def infer(file):
    """Given a file-like object, infer the columns types and return a list
    of sqlalchemy columns."""

    # First get the number of columns that exist
    first_row = pandas.read_csv(file, nrows=1)
    columns_length = first_row.shape[1]
    file.seek(0)

    # Instruct pandas to try and parse dates out of every column
    cols = list(range(0, columns_length))
    datetime_discovery = pandas.read_csv(file, parse_dates=cols, nrows=1000)
    file.seek(0)

    # Record the columns able to be converted to datetimes
    date_columns = []
    for k, v in datetime_discovery.dtypes.items():
        if v.type == numpy.datetime64:
            date_columns.append(k)

    # Infer the rest of the column types knowing which are datetimes
    df = pandas.read_csv(file, parse_dates=date_columns, nrows=1000)
    file.seek(0)

    # Convert the numpy data types to sqlalchemy data types
    # Create column objects from the inferred types
    columns = []
    for k, v in df.dtypes.items():
        sqlalchemy_type = numpy_sqlalchemy_type_map[v.type]
        slug = slugify(k)
        column = Column(slug, sqlalchemy_type, nullable=True)
        columns.append(column)

    return columns
