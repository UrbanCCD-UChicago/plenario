import json
import logging

from collections import namedtuple
from datetime import datetime, timedelta
from dateutil import parser
from marshmallow import fields, Schema, ValidationError
from marshmallow.validate import Range, Length, OneOf

from plenario.api.common import extract_first_geometry_fragment, make_fragment_str
from plenario.database import session
from plenario.models import MetaTable, ShapeMetadata


class Validator(Schema):

    valid_aggs = {'day', 'week', 'month', 'quarter', 'year'}
    valid_formats = {'csv', 'geojson', 'json'}

    agg = fields.Str(default='week', validate=OneOf(valid_aggs))
    buffer = fields.Integer(default=100, validate=Range(0))
    dataset_name = fields.Str(default=None, validate=OneOf(MetaTable.index()), dump_to='dataset')
    dataset_name__in = fields.List(fields.Str(), default=MetaTable.index(), validate=Length(1))  # TODO: Improve.
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

    dataset_name = fields.Str(default=None, validate=OneOf(MetaTable.index()), dump_to='dataset', required=True)


class NoDefaultDatesValidator(Validator):

    obs_date__ge = fields.Date(default=None)
    obs_date__le = fields.Date(default=None)


ValidatorResult = namedtuple('ValidatorResult', 'data errors warnings')

converters = {
    'buffer': int,
    'dataset': lambda x: MetaTable.get_by_dataset_name(x),
    'dataset_name__in': lambda x: x.split(','),
    'date__time_of_day_ge': int,
    'date__time_of_day_le': int,
    'obs_date__ge': lambda x: parser.parse(x).date(),
    'obs_date__le': lambda x: parser.parse(x).date(),
    'offset': int,
    'resolution': int,
    'geom': lambda x: make_fragment_str(extract_first_geometry_fragment(x)),
    'shape': lambda x: get_shape_table(x)
}


# TODO: Get rid of this eventually, only here because ShapeMetadata lacks this method.
def get_shape_table(shtable_name):
    return session.query(ShapeMetadata)\
        .filter(ShapeMetadata.dataset_name == shtable_name)\
        .first()\
        .shape_table


def convert(request_args):
    for key, value in request_args.items():
        try:
            request_args[key] = converters[key](value)
        except:
            # Failed transactions, which we do expect, can cause
            # a DatabaseError with Postgres. Failing to rollback
            # prevents further queries from being carried out.
            session.rollback()


def validate(validator_cls, request_args, defaults=True):

    if defaults:
        # this validator will return results/defaults for all fields
        validator = validator_cls()
    else:
        # this validator will return results/defaults for only the fields
        # provided in the request
        validator = validator_cls(only=request_args.keys())

    # convert string values to corresponding types so the validator can work
    convert(request_args)
    # returns validated parameters as strings
    result = validator.dump(request_args)
    # convert strings back to correct types
    convert(result.data)

    # determine unchecked parameters provided in the request
    unchecked = set(request_args.keys()) - set(validator.fields.keys())

    # determine if a dataset was provided to grab the column names
    columns = []
    if 'dataset_name' in request_args:
        # this little bit of result formatting helps make things easier, but I
        # eventually need to find a better place for it
        result.data['metatable'] = result.data['dataset']
        result.data['dataset'] = result.data['dataset'].point_table
        columns += result.data['dataset'].columns.keys()
    if 'shape' in request_args:
        columns += result.data['shape'].columns.keys()

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
