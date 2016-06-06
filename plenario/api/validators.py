import dateutil.parser

from datetime import datetime, timedelta
from sqlalchemy import Table
from sqlalchemy.exc import NoSuchTableError

from plenario.api.common import extract_first_geometry_fragment, make_fragment_str
from plenario.api.errors import bad_request
from plenario.database import app_engine, Base, session
from plenario.models import ShapeMetadata


# ==============
# ParamValidator
# ==============

class ParamValidator(object):

    def __init__(self, dataset_name=None, shape_dataset_name=None):
        # Maps param keys to functions that validate and transform its string value.
        # Each transform returns (transformed_value, error_string)
        self.transforms = {}
        # Map param keys to usable values.
        self.vals = {}
        # Let the caller know which params we ignored.
        self.warnings = []

        if dataset_name:
            # Throws NoSuchTableError. Should be caught by caller.
            self.dataset = Table(dataset_name, Base.metadata, autoload=True,
                                 autoload_with=app_engine, extend_existing=True)
            self.cols = self.dataset.columns.keys()
            # SQLAlchemy boolean expressions
            self.conditions = []

            if shape_dataset_name is not None:
                self.set_shape(shape_dataset_name)

    def set_shape(self, shape_dataset_name):
        shape_table_meta = session.query(ShapeMetadata).get(shape_dataset_name)
        if shape_table_meta:
            shape_table = shape_table_meta.shape_table
            self.cols += ['{}.{}'.format(shape_table.name, key) for key in shape_table.columns.keys()]
            self.vals['shape'] = shape_table

    def set_optional(self, name, transform, default):
        """
        :param name: Name of expected HTTP parameter
        :param transform: Function of type
                          f(param_val: str) -> (validated_argument: Option<object, None>, err_msg: Option<str, None>
                          Return value should be of form (output, None) if transformation was applied successully,
                          or (None, error_message_string) if transformation could not be applied.
        :param default: Value to apply to associate with parameter given by :name
                        if not specified by user. Can be a callable.
        :return: Returns the Validator to allow generative construction.
        """
        self.vals[name] = default
        self.transforms[name] = transform

        # For call chaining
        return self

    def validate(self, params):
        for k, v in params.items():
            if k in self.transforms.keys():
                # k is a param name with a defined transformation
                # Get the transformation and apply it to v
                val, err = self.transforms[k](v)
                if err:
                    # v wasn't a valid string for this param name
                    return err
                # Override the default with the transformed value.
                self.vals[k] = val
                continue

            elif hasattr(self, 'cols'):
                # Maybe k specifies a condition on the dataset
                cond, err = self._make_condition(k, v)
                # 'if cond' fails because sqlalchemy overrides __bool__
                if cond is not None:
                    self.conditions.append(cond)
                    continue
                elif err:
                    # Valid field was specified, but operator was malformed
                    return err
                    # else k wasn't an attempt at setting a condition

            # This param is neither present in the optional params
            # nor does it specify a field in this dataset.
            if k != 'shape':
                # quick and dirty way to make sure 'shape' is not listed as an unused value
                warning = 'Unused parameter value "{}={}"'.format(k, v)
                self.warnings.append(warning)

        self._eval_defaults()

    def get_geom(self):
        validated_geom = self.vals['location_geom__within']
        if validated_geom is not None:
            buff = self.vals.get('buffer', 100)
            return make_fragment_str(validated_geom, buff)

    def _eval_defaults(self):
        """
        Replace every value in vals that is callable with the returned value of that callable.
        Lets us lazily evaluate dafaults only when they aren't overridden.
        """
        for k, v in self.vals.items():
            if hasattr(v, '__call__'):
                self.vals[k] = v()

    # Map codes we accept in API docs to sqlalchemy function names
    field_ops = {
        'gt': '__gt__',
        'ge': '__ge__',
        'lt': '__lt__',
        'le': '__le__',
        'ne': '__ne__',
        'like': 'like',
        'ilike': 'ilike',
    }

    def _check_shape_condition(self, field):
        # returns false if the shape column is
        return self.vals.get('shape') is not None \
               and '{}.{}'.format(self.vals['shape'], field) in self.cols

    def _make_condition(self, k, v):
        # Generally, we expect the form k = [field]__[op]
        # Can also be just [field] in the case of simple equality
        tokens = k.split('__')
        # An attribute of the dataset
        field = tokens[0]
        if field not in self.cols and not self._check_shape_condition(field):
            # No column matches this key.
            # Rather than return an error here,
            # we'll return None to indicate that this field wasn't present
            # and let the calling function send a warning to the client.

            return None, None

        col = self.dataset.columns.get(field)

        if len(tokens) == 1:
            # One token? Then it's an equality operation of the form k=v
            # col == v creates a SQLAlchemy boolean expression
            return (col == v), None
        elif len(tokens) == 2:
            # Two tokens? Then it's of the form [field]__[op_code]=v
            op_code = tokens[1]
            valid_op_codes = self.field_ops.keys() + ['in']
            if op_code not in valid_op_codes:
                error_msg = "Invalid dataset field operator:" \
                            " {} called in {}={}".format(op_code, k, v)
                return None, error_msg
            else:
                cond = self._make_condition_with_operator(col, op_code, v)
                return cond, None

        else:
            error_msg = "Too many arguments on dataset field {}={}" \
                        "\n Expected [field]__[operator]=value".format(k, v)
            return None, error_msg

    def _make_condition_with_operator(self, col, op_code, target_value):
        if op_code == 'in':
            cond = col.in_(target_value.split(','))
            return cond
        else:   # Any other op code
            op_func = self.field_ops[op_code]
            # op_func is the name of a method bound to the SQLAlchemy column object.
            # Get the method and call it to create a binary condition (like name != 'Roy')
            # on the value the user specified.
            cond = getattr(col, op_func)(target_value)
            return cond


# =========================
# Validator transformations
# =========================
# To be added to a validator object with Validator.set_optional()
# They take a string and return a tuple of (expected_return_type, error_message)
# where error_message is non-None only when the transformation could not produce an object of the expected type.

def setup_detail_validator(dataset_name, params):

    try:
        if 'shape' in params:
            shape = params['shape']
        else:
            shape = None
        validator = ParamValidator(dataset_name, shape)
    except NoSuchTableError:
        return bad_request("Cannot find dataset named {}".format(dataset_name))

    validator \
        .set_optional('obs_date__ge',
                      date_validator,
                      datetime.now() - timedelta(days=90)) \
        .set_optional('obs_date__le', date_validator, datetime.now()) \
        .set_optional('location_geom__within', geom_validator, None) \
        .set_optional('offset', int_validator, 0) \
        .set_optional('data_type',
                      make_format_validator(['json', 'csv', 'geojson']),
                      'json') \
        .set_optional('date__time_of_day_ge', time_of_day_validator, 0) \
        .set_optional('date__time_of_day_le', time_of_day_validator, 23)

    '''create another validator to check if shape dataset is in meta_shape, then return
    the actual table object for that shape if it is present'''

    return validator


def agg_validator(agg_str):  #
    valid_agg = ['day', 'week', 'month', 'quarter', 'year']  #

    if agg_str in valid_agg:  #
        return agg_str, None  #
    else:
        error_msg = '{} is not a valid unit of aggregation. Plenario accepts {}'\
                    .format(agg_str, ','.join(valid_agg))  #
        return None, error_msg  #


def date_validator(date_str):
    try:
        date = dateutil.parser.parse(date_str)
        return date, None
    except (ValueError, OverflowError):
        error_msg = 'Could not parse date string {}'.format(date_str)
        return None, error_msg


def list_of_datasets_validator(list_str):
    table_names = list_str.split(',')
    if not len(table_names) > 1:
        error_msg = "Expected comma-separated list of computer-formatted dataset names." \
                    " Couldn't parse {}".format(list_str)
        return None, error_msg
    return table_names, None


def make_format_validator(valid_formats):
    """
    :param valid_formats: A list of strings that are acceptable types of data formats.
    :return: a validator function usable by ParamValidator
    """

    def format_validator(format_str):
        if format_str in valid_formats:
            return format_str, None
        else:
            error_msg = '{} is not a valid output format. Plenario accepts {}' \
                .format(format_str, ','.join(valid_formats))
            return error_msg, None

    return format_validator


def geom_validator(geojson_str):
    # Only extracts first geometry fragment as dict.
    try:
        fragment = extract_first_geometry_fragment(geojson_str)
        return fragment, None
    except ValueError:
        error_message = "Could not parse as geojson: {}".format(geojson_str)
        return None, error_message


def int_validator(int_str):
    try:
        num = int(int_str)
        assert(num >= 0)
        return num, None
    except (ValueError, AssertionError):
        error_message = "Could not parse as non-negative integer: {}".format(int_str)
        return None, error_message


def time_of_day_validator(hour_str):
    num, err = int_validator(hour_str)
    if err:
        return None, err
    if num > 23:
        error_message = "{} is not a valid hour of the day (Must be between 0 and 23)".format(hour_str)
        return None, error_message
    else:
        return num, None


def no_op_validator(foo):
    return foo, None
