import json

from dateutil import parser
from plenario.api.common import make_fragment_str, extract_first_geometry_fragment
from plenario.models import MetaTable
from marshmallow.fields import Field
from marshmallow.exceptions import ValidationError


class DateTime(Field):
    def _serialize(self, value, attr, obj):
        return value.isoformat()

    def _deserialize(self, value, attr, data):
        try:
            return parser.parse(value)
        except ValueError:
            raise ValidationError('{} does not contain a date'.format(value))


class Geometry(Field):

    def _serialize(self, value, attr, obj):
        return json.loads(value)

    def _deserialize(self, value, attr, data):
        try:
            return make_fragment_str(extract_first_geometry_fragment(value))
        except (ValueError, AttributeError):
            raise ValidationError('Invalid geom: {}'.format(value))


class Pointset(Field):

    def _serialize(self, value, attr, obj):
        return value.name

    def _deserialize(self, value, attr, data):
        try:
            return MetaTable.get_by_dataset_name(value).point_table
        except AttributeError:
            raise ValidationError('{} is not a valid dataset'.format(value))

