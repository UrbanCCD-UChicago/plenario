from collections import namedtuple
from datetime import datetime, timedelta
from dateutil import parser
from plenario.api.common import extract_first_geometry_fragment
from plenario.models import MetaTable

from marshmallow import fields, Schema, ValidationError
from marshmallow.validate import Range, Length, OneOf


def geom_validator(geojson_str):
    try:
        return extract_first_geometry_fragment(geojson_str)
    except ValueError:
        raise ValidationError('Could not parse as geojson {}'.format(geojson_str))


ValidatorResult = namedtuple('ValidatorResult', 'data errors warnings')


class ValidatorSchema(Schema):

    valid_aggs = {'day', 'week', 'month', 'quarter', 'year'}
    valid_formats = {'csv', 'geojson', 'json'}

    agg = fields.Str(default='week', validate=OneOf(valid_aggs))
    buffer = fields.Integer(default=100, validate=Range(0))
    dataset_name = fields.Str(default=None, validate=OneOf(MetaTable.index()))
    dataset_name__in = fields.List(fields.Str(), default=MetaTable.index(), validate=Length(1))  # TODO: Improve.
    date__time_of_day_ge = fields.Integer(default=0, validate=Range(0, 23))
    date__time_of_day_le = fields.Integer(default=23, validate=Range(0, 23))
    data_type = fields.Str(default='json', validate=OneOf(valid_formats))
    location_geom__within = fields.Str(default=None, validate=geom_validator, dump_to='geom')
    obs_date__ge = fields.Date(default=datetime.now() - timedelta(days=90))
    obs_date__le = fields.Date(default=datetime.now())
    offset = fields.Integer(default=0, validate=Range(0))
    resolution = fields.Integer(default=500, validate=Range(0))

    @staticmethod
    def convert(request_args):
        for key, value in request_args.items():
            try:
                request_args[key] = converters[key](value)
            except:
                pass


converters = {
    'buffer': int,
    'dataset_name__in': lambda x: x.split(','),
    'date__time_of_day_ge': int,
    'date__time_of_day_le': int,
    'obs_date__ge': parser.parse,
    'obs_date__le': parser.parse,
    'offset': int,
    'resolution': int
    # TODO: Geom.
    # TODO: Shape.
}


def validate(request, *consider):

    # only validate arguments in consider, if provided
    validator = ValidatorSchema()
    if consider:
        validator = ValidatorSchema(only=consider)

    # convert request argument strings to expected types
    request_args = request.args.to_dict()
    validator.convert(request_args)

    # validate request arguments and create result object
    result = validator.dump(request_args)

    # report on unused parameters provided in the request
    warnings = []
    unused_params = set(request_args.keys()) - set(result.data.keys())
    for param in unused_params:
        warnings.append('Unused parameter value "{}={}"'
                        .format(param, request_args[param]))

    return ValidatorResult(result.data, result.errors, warnings)
