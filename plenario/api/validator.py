from collections import namedtuple
from datetime import datetime, timedelta
from dateutil import parser
from marshmallow import fields, Schema
from marshmallow.validate import Range, Length, OneOf
from sqlalchemy.exc import DatabaseError, ProgrammingError

from plenario.api.common import extract_first_geometry_fragment, make_fragment_str
from plenario.database import session
from plenario.models import MetaTable, ShapeMetadata


class Validator(Schema):
    """Base validator object using Marshmallow. Don't be intimidated! As scary
    as the following block of code looks it's quite simple, and saves us from
    writing validators. Let's break it down...

    <FIELD_NAME> = fields.<TYPE>(default=<DEFAULT_VALUE>, validate=<VALIDATOR FN>)

    The validator, when instanciated, has a method called 'dump'.which expects a
    dictionary of arguments, where keys correspond to <FIELD_NAME>. The validator
    has a default <TYPE> checker, that along with extra <VALIDATOR FN>s will
    accept or reject the value associated with the key. If the value is missing
    or rejected, the validator will substitue it with the value specified by
    <DEFAULT_VALUE>."""

    valid_aggs = {'day', 'week', 'month', 'quarter', 'year'}
    valid_formats = {'csv', 'geojson', 'json'}

    agg = fields.Str(default='week', validate=OneOf(valid_aggs))
    buffer = fields.Integer(default=100, validate=Range(0))
    dataset_name = fields.Str(default=None, validate=OneOf(MetaTable.index()), dump_to='dataset')
    dataset_name__in = fields.List(fields.Str(), default=MetaTable.index(), validate=Length(1))
    date__time_of_day_ge = fields.Integer(default=0, validate=Range(0, 23))
    date__time_of_day_le = fields.Integer(default=23, validate=Range(0, 23))
    data_type = fields.Str(default='json', validate=OneOf(valid_formats))
    location_geom__within = fields.Str(default=None, dump_to='geom')
    obs_date__ge = fields.Date(default=datetime.now() - timedelta(days=90))
    obs_date__le = fields.Date(default=datetime.now())
    offset = fields.Integer(default=0, validate=Range(0))
    resolution = fields.Integer(default=500, validate=Range(0))
    shape = fields.Str(default=None, validate=OneOf(ShapeMetadata.index()))


class DatasetRequiredValidator(Validator):
    """Some endpoints, like /detail-aggregate, should not be allowed to recieve
    requests that do not specify a 'dataset_name' in the query string."""

    dataset_name = fields.Str(default=None, validate=OneOf(MetaTable.index()), dump_to='dataset', required=True)


class NoGeoJSONValidator(Validator):
    """Some endpoints, like /timeseries, should not allow GeoJSON as a valid
    response format."""

    valid_formats = {'csv', 'json'}


class NoGeoJSONDatasetRequiredValidator(DatasetRequiredValidator):
    """Some endpoints, like /detail-aggregate, should not allow GeoJSON as a valid
    response format and require a dataset."""

    valid_formats = {'csv', 'json'}


class NoDefaultDatesValidator(Validator):
    """Some endpoints, specifically /datasets, will not return results with
    the original default dates (because the time window is so small)."""

    obs_date__ge = fields.Date(default=None)
    obs_date__le = fields.Date(default=None)


# ValidatorResult
# ===============
# Many methods in response.py rely on information that used to be provided
# by the old ParamValidator attributes. This namedtuple carries that same
# info around, and allows me to not have to rewrite any response code.

ValidatorResult = namedtuple('ValidatorResult', 'data errors warnings')


# converters
# ==========
# Callables which are used to convert request arguments to their correct types.

converters = {
    'buffer': int,
    'dataset': lambda x: MetaTable.get_by_dataset_name(x),
    'shape': lambda x: ShapeMetadata.get_by_dataset_name(x).shape_table,
    'dataset_name__in': lambda x: x.split(','),
    'date__time_of_day_ge': int,
    'date__time_of_day_le': int,
    'obs_date__ge': lambda x: parser.parse(x).date(),
    'obs_date__le': lambda x: parser.parse(x).date(),
    'offset': int,
    'resolution': int,
    'geom': lambda x: make_fragment_str(extract_first_geometry_fragment(x)),
}


def convert(request_args):
    """Convert a dictionary of arguments from strings to their types. How the
    values are converted are specified by the converters dictionary defined
    above.

    :param request_args: dictionary of request arguments

    :returns: converted dictionary"""

    for key, value in request_args.items():
        try:
            request_args[key] = converters[key](value)
        except (KeyError, TypeError, AttributeError):
            pass
        except (DatabaseError, ProgrammingError):
            # Failed transactions, which we do expect, can cause
            # a DatabaseError with Postgres. Failing to rollback
            # prevents further queries from being carried out.
            session.rollback()


def validate(validator, request_args):
    """Validate a dictionary of arguments. Substitute all missing fields with
    defaults if not explicitly told to do otherwise.

    :param validator: what kind of validator to use
    :param request_args: dictionary of arguments from a request object

    :returns: ValidatorResult namedtuple"""

    args = request_args.copy()

    # convert string values to corresponding types so the validator can work
    convert(args)
    # returns validated parameters as strings
    result = validator.dump(args)
    # convert any remaining strings in args to the right type
    convert(result.data)

    # determine unchecked parameters provided in the request
    unchecked = set(args.keys()) - set(validator.fields.keys())

    # determine if a dataset was provided to grab the column names
    columns = []
    if 'dataset_name' in request_args:
        # this little bit of result formatting helps make things easier, but I
        # eventually need to find a better place for it
        result.data['metatable'] = result.data['dataset']
        result.data['dataset'] = result.data['dataset'].point_table
        columns += result.data['dataset'].columns.keys()

    warnings = []
    # parameters that have yet to be checked could still possibly apply to
    # a table column
    for param in unchecked:
        tokens = param.split('__')
        # if it is a column, pass it through for use in the condition builder
        if tokens[0] in columns:
            result.data[param] = request_args[param]
        # otherwise mark it down with a warning, let them know that we ignored it
        else:
            warnings.append('Unused parameter value "{}={}"'
                            .format(param, request_args[param]))

    return ValidatorResult(result.data, result.errors, warnings)
