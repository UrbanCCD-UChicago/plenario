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


ValidatorResult = namedtuple('ValidatorResult', 'data errors warnings')

converters = {
    'buffer': int,
    'dataset': lambda x: MetaTable.get_by_dataset_name(x),
    'dataset_name__in': lambda x: x.split(','),
    'date__time_of_day_ge': int,
    'date__time_of_day_le': int,
    'obs_date__ge': parser.parse,
    'obs_date__le': parser.parse,
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


def validate(validator_cls, request_args, *consider):

    # create instance of the kind of validator we're using
    if consider:
        validator = validator_cls(only=consider)
    else:
        validator = validator_cls()

    # convert the request argument strings into correct types
    convert(request_args)

    # validate the request arguments (converts arguments back to strings)
    result = validator.dump(request_args)

    # determine unused parameters provided in the request
    validator_attrs = validator.fields.keys()
    request_attrs = request_args.keys()
    unused_params = set(request_attrs) - set(validator_attrs)

    # convert string values in data back to correct types
    convert(result.data)

    # determine if a specific dataset is provided
    dataset = None
    shapeset = None
    if result.data['dataset'] is not None:
        dataset = result.data['dataset'].point_table
    if result.data['shape'] is not None:
        shapeset = result.data['shape']

    # check that the param is meant to be used as a column condition
    warnings = []
    for param in unused_params:
        tokens = param.split('__')  # have to consider operators!
        columns = dataset.columns.keys()
        if shapeset is not None:
            columns += shapeset.columns.keys()
        if dataset is None or tokens[0] not in columns:
            warnings.append('Unused parameter value "{}={}"'.
                            format(param, request_args[param]))
        else:
            result.data[param] = request_args[param]

    return ValidatorResult(result.data, result.errors, warnings)
