import numpy
import pandas
from sqlalchemy import Column

from plenario.models.meta.types import numpy_sqlalchemy_type_map
from plenario.utils.helpers import slugify


def count_columns(source):

    return pandas.read_csv(source, nrows=1).shape[1]


def get_names_of_datetime_columns(source, number_of_columns):

    cols = list(range(0, number_of_columns))
    datetime_discovery = pandas.read_csv(source, parse_dates=cols, nrows=1000)

    datetime_columns = []
    for k, v in datetime_discovery.dtypes.items():
        if v.type == numpy.datetime64:
            datetime_columns.append(k)

    return datetime_columns


def infer_with_datetime_columns(source, datetime_columns):

    df = pandas.read_csv(source, parse_dates=datetime_columns, nrows=1000)

    columns = []
    for k, v in df.dtypes.items():
        sqlalchemy_type = numpy_sqlalchemy_type_map[v.type]
        slug = slugify(k)
        column = Column(slug, sqlalchemy_type, nullable=True)
        columns.append(column)

    return columns


def infer_local(file):

    columns_length = count_columns(file)
    file.seek(0)
    datetime_columns = get_names_of_datetime_columns(file, columns_length)
    file.seek(0)
    columns = infer_with_datetime_columns(file, datetime_columns)
    file.seek(0)

    return columns


def infer_remote(url):

    columns_length = count_columns(url)
    datetime_columns = get_names_of_datetime_columns(url, columns_length)
    return infer_with_datetime_columns(url, datetime_columns)


def infer(source):

    try:
        return infer_remote(source)
    except:
        return infer_local(source)
