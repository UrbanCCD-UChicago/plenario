import json

from collections import namedtuple
from datetime import datetime, timedelta
from marshmallow import fields, Schema
from marshmallow.validate import Range, ValidationError
from psycopg2 import Error
from sqlalchemy.exc import DatabaseError, ProgrammingError, NoSuchTableError
from sqlalchemy import MetaData, Table

from plenario.api.common import extract_first_geometry_fragment, make_fragment_str
from plenario.sensor_network.api.sensor_condition_builder import field_ops
from plenario.database import session, redshift_engine
from plenario.sensor_network.sensor_models import NodeMeta, NetworkMeta, FeatureOfInterest, Sensor


def validate_network(network):
    if network not in NetworkMeta.index():
        raise ValidationError("Invalid network name: {}.".format(network))


def validate_nodes(nodes):
    if isinstance(nodes, basestring):
        nodes = [nodes]
    valid_nodes = NodeMeta.index()
    for node in nodes:
        if node not in valid_nodes:
            raise ValidationError("Invalid node ID: {}.".format(node))


def validate_features(features):
    if isinstance(features, basestring):
        features = [features]
    valid_features = FeatureOfInterest.index()
    for feature in features:
        if feature not in valid_features:
            raise ValidationError("Invalid feature name: {}.".format(feature))


def validate_sensors(sensors):
    if isinstance(sensors, basestring):
        sensors = [sensors]
    valid_sensors = Sensor.index()
    for sensor in sensors:
        if sensor not in valid_sensors:
            raise ValidationError("Invalid sensor name: {}.".format(sensor))


# not working...
def validate_geom(geom):
    """Custom validator for geom parameter."""

    try:
        return extract_first_geometry_fragment(geom)
    except Error:
        raise ValidationError("Could not parse geojson: {}.".format(geom))


class Validator(Schema):
    """Base validator object using Marshmallow. Don't be intimidated! As scary
    as the following block of code looks it's quite simple, and saves us from
    writing validators. Let's break it down...

    <FIELD_NAME> = fields.<TYPE>(default=<DEFAULT_VALUE>, validate=<VALIDATOR FN>)

    The validator, when instanciated, has a method called 'dump'.which expects a
    dictionary of arguments, where keys correspond to <FIELD_NAME>. The validator
    has a default <TYPE> checker, that along with extra <VALIDATOR FN>s will
    accept or reject the value associated with the key. If the value is missing
    or rejected, the validator will substitute it with the value specified by
    <DEFAULT_VALUE>."""

    network_name = fields.Str(allow_none=True, missing=None, default='ArrayOfThings', validate=validate_network)

    # For observations:
    #
    # only validates that nodes, features, and sensors exist, not that they are part of the correct network
    # fills in None as default, handled by validate(),
    # which fills in all nodes, features, and sensors in the correct network
    nodes = fields.List(fields.Str(), default=None, validate=validate_nodes)
    features_of_interest = fields.List(fields.Str(), default=None, validate=validate_features)
    sensors = fields.List(fields.Str(), default=None, validate=validate_sensors)

    # For metadata:
    node_id = fields.Str(default=None, validate=validate_nodes)
    feature = fields.Str(default=None, validate=validate_features)
    sensor = fields.Str(default=None, validate=validate_sensors)

    location_geom__within = fields.Str(default=None, dump_to='geom', validate=validate_geom)
    start_datetime = fields.DateTime(default=datetime.utcnow() - timedelta(days=90))
    end_datetime = fields.DateTime(default=datetime.utcnow())
    filter = fields.Str(allow_none=True, missing=None, default=None)
    limit = fields.Integer(default=1000)
    offset = fields.Integer(default=0, validate=Range(0))


class SensorNetworkValidator(Validator):
    """Validator for retrieving sensor network metadata"""


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
    'end_datetime': lambda x: x.isoformat().split('+')[0]
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

    :param validator: type of validator to use
    :param request_args: dictionary of arguments from a request object

    :returns: ValidatorResult namedtuple"""

    args = request_args.copy()

    # If there are errors, fail quickly and return.
    result = validator.load(args)
    if result.errors:
        return result

    # If all arguments are valid, fill in validator defaults.
    result = validator.dump(result.data)

    # fill in all nodes, sensors, and features within the network as a default
    # if this is not an observation query, KeyErrors will result
    try:
        network_name = result.data['network_name']
        if result.data['nodes'] is None:
            result.data['nodes'] = NodeMeta.index(network_name)
        if result.data['features_of_interest'] is None:
            result.data['features_of_interest'] = FeatureOfInterest.index(network_name)
        if result.data['sensors'] is None:
            result.data['sensors'] = Sensor.index(network_name)
    except KeyError:
        pass

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
    if 'filter NOT FUNCTIONAL' in request_args:

        # Report a tree which causes the JSON parser to fail.
        # Or a tree whose value is not valid.
        try:
            cond_tree = json.loads(request_args['filter'])
            meta = MetaData()
            if valid_tree(meta, cond_tree):
                result.data['filter'] = cond_tree
        except (ValueError, KeyError) as err:
            result.errors['filter'] = "Bad tree: {} -- causes error {}.".format(request_args['filter'], err)
            return result

    if unchecked:
        for param in unchecked:
            result.errors[param] = 'Not a valid filter'.format(param)

    # ValidatorResult(dict, dict, list)
    return ValidatorResult(result.data, result.errors, warnings)


def valid_tree(meta, tree):
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
        return all([valid_tree(meta, subtree) for subtree in tree['val']])

    elif op in field_ops:
        col = tree.get('col')
        val = tree.get('val')

        if col is None or val is None:
            err_msg = 'Missing or invalid keyword in {}'.format(tree)
            err_msg += ' -- use format "{\'op\': OP, \'col\': COL, \'val\', VAL}"'
            raise ValueError(err_msg)

        try:
            (table_name, col_name) = col.split('.')
            table_name = table_name.lower()
            table = Table(table_name, meta, autoload=True, autoload_with=redshift_engine)
        except (AttributeError, NoSuchTableError):
            raise ValueError("Invalid feature of interest {} in filter".format(table_name))

        return valid_column_condition(table, col_name, val)

    else:
        raise ValueError("Invalid operation {}".format(op))


def valid_column_condition(table, column_name, value):
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

    if column_name in table.columns.keys():
        column = table.columns[column_name]
    else:
        raise KeyError("Invalid column name {}".format(column_name))

    try:
        if type(value) != column.type.python_type:
            column.type.python_type(value)  # Blessed Python.
    except (ValueError, TypeError):
        raise ValueError("Invalid value type for {}. Was expecting {}"
                         .format(value, column.type.python_type))

    return True
