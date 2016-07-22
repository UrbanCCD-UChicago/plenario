import json
import ast

from collections import namedtuple
from datetime import datetime, timedelta
from dateutil import parser
from marshmallow import fields, Schema
from marshmallow.validate import OneOf, ValidationError
from psycopg2 import Error
from sqlalchemy.exc import DatabaseError, ProgrammingError, NoSuchTableError

from plenario.api.common import extract_first_geometry_fragment, make_fragment_str
from plenario.api.condition_builder import field_ops
from plenario.database import session
from plenario.sensor_network.sensor_models import NodeMeta, NetworkMeta, Observation


def validate_nodes(nodes):
    """Custom validator for nodes parameter."""

    valid_nodes = NodeMeta.index()
    for node in nodes:
        if node not in valid_nodes:
            raise ValidationError("Invalid node ID: {}.".format(node))


# not working...
def validate_geom(geom):
    """Custom validator for geom parameter."""

    try:
        fragment = extract_first_geometry_fragment(geom)
    except Error:
        return False
    return True


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

    location_geom__within = fields.Str(default=None, dump_to='geom', validate=validate_geom)
    network_name = fields.Str(allow_none=True, missing=None, default=None, validate=OneOf(NetworkMeta.index()))
    node_id = fields.Str(default=None, validate=OneOf(NodeMeta.index()))
    nodes = fields.List(fields.Str(), default=NodeMeta.index(), validate=validate_nodes)
    start_datetime = fields.DateTime(default=datetime.now() - timedelta(days=90))
    end_datetime = fields.DateTime(default=datetime.now())


class NoGeoJSONValidator(Validator):
    """Some endpoints, like /timeseries, should not allow GeoJSON as a valid
    response format."""

    valid_formats = {'csv', 'json'}
    # Validator re-initialized so that it doesn't use old valid_formats.
    data_type = fields.Str(default='json', validate=OneOf(valid_formats))


class ExportFormatsValidator(Validator):
    """For /shapes/<shapeset_name>?data_type=<format>"""

    valid_formats = {'shapefile', 'kml', 'json'}
    data_type = fields.Str(default='json', validate=OneOf(valid_formats))


class SensorNetworkValidator(Validator):
    """For sensor networks"""

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
    'geom': lambda x: make_fragment_str(extract_first_geometry_fragment(x)),
    'start_datetime': lambda x: x.isoformat().split('+')[0],
    'end_datetime': lambda x: x.isoformat().split('+')[0],
}


def convert(request_args):
    """Convert a dictionary of arguments from strings to their types. How the
    values are converted are specified by the converters dictionary defined
    above.

    :param request_args: dictionary of request arguments
    :param converters: dictionary of converter functions

    :returns: converted dictionary"""

    for key, value in request_args.items():
        try:
            request_args[key] = converters[key](value)
        except (KeyError, TypeError, AttributeError, NoSuchTableError):
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
    :param args: dictionary of arguments from a request object

    :returns: ValidatorResult namedtuple"""

    args = request_args.copy()

    # If there are errors, fail quickly and return.
    result = validator.load(args)
    if result.errors:
        return result

    # If all arguments are valid, fill in validator defaults.
    result = validator.dump(result.data)

    # Certain values will be dumped as strings. This conversion
    # makes them into their corresponding type. (ex. Table)
    convert(result.data)

    # Holds messages concerning unnecessary parameters. These can be either
    # junk parameters, or redundant column parameters if a tree filter was
    # used.
    warnings = []

    # Determine unchecked parameters provided in the request.
    unchecked = set(args.keys()) - set(validator.fields.keys())

    # If tree filters were provided, ignore ALL unchecked parameters that are
    # not tree filters or response format information.

    if 'filter' in request_args.keys():

        # Report a tree which causes the JSON parser to fail.
        # Or a tree whose value is not valid.
        try:
            cond_tree = json.loads(request_args['filter'])
            if valid_tree(cond_tree):
                result.data['filter'] = cond_tree
        except (ValueError, KeyError) as err:
            result.errors['filter'] = "Bad tree: {} -- causes error {}.".format(request_args['filter'], err)
            return result

    # ValidatorResult(dict, dict, list)
    return ValidatorResult(result.data, result.errors, warnings)

def valid_tree(tree):
    """Given a dictionary containing a condition tree, validate all conditions
    nestled in the tree.

    :param table: table to build conditions for, need it for the columns
    :param tree: condition_tree

    :returns: boolean value, true if the tree is valid"""

    if not tree.keys():
        raise ValueError("Empty or malformed tree.")

    op = tree.get('op')
    if not op:
        raise ValueError("Invalid keyword in {}".format(tree))

    if op == "and" or op == "or":
        return all([valid_tree(subtree) for subtree in tree['val']])

    elif op in field_ops:
        col = tree.get('col')
        val = tree.get('val')

        if col is None or val is None:
            err_msg = 'Missing or invalid keyword in {}'.format(tree)
            err_msg += ' -- use format "{\'op\': OP, \'col\': COL, \'val\', VAL}"'
            raise ValueError(err_msg)

        return valid_column_condition(col, val)

    else:
        raise ValueError("Invalid operation {}".format(op))

def valid_column_condition(column_name, value):
    """Establish whether or not a set of components is able to make a valid
    condition for the provided table.

    :param table: SQLAlchemy table object
    :param column_name: Name of the column
    :param value: target value"""

    # This is mostly to deal with what a pain datetime is.
    # I can't just use its type to cast a string. :(

    condition = {column_name: value}
    convert(condition)
    value = condition[column_name]

    if column_name in Observation.__dict__.keys():
        column = Observation.__table__.c[column_name]
    else:
        raise KeyError("Invalid column name {}".format(column_name))

    try:
        if type(value) != column.type.python_type:
            column.type.python_type(value)  # Blessed Python.
    except (ValueError, TypeError):
        raise ValueError("Invalid value type for {}. Was expecting {}"
                         .format(value, column.type.python_type))

    return True
