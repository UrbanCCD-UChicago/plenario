import sqlalchemy
import numpy


numpy_sqlalchemy_type_map = {
    numpy.bool_: sqlalchemy.Boolean,
    numpy.datetime64: sqlalchemy.DateTime,
    numpy.float32: sqlalchemy.Float,
    numpy.float64: sqlalchemy.Float,
    numpy.int64: sqlalchemy.Integer,
    numpy.object_: sqlalchemy.String
}