import json
import re
from collections import namedtuple
from datetime import datetime, timedelta

import sqlalchemy
from dateutil import parser
from marshmallow import fields, Schema
from marshmallow.validate import Range, OneOf, ValidationError
from sqlalchemy import MetaData
from sqlalchemy.exc import DatabaseError, ProgrammingError, NoSuchTableError

from plenario.api.common import extract_first_geometry_fragment, make_fragment_str
from plenario.api.condition_builder import field_ops
from plenario.database import postgres_session, redshift_engine
from plenario.models import ShapeMetadata, MetaTable
from plenario.models.SensorNetwork import NodeMeta, NetworkMeta, FeatureMeta, SensorMeta
from plenario.sensor_network.api.sensor_aggregate_functions import aggregate_fn_map
from plenario.utils.helpers import reflect
from plenario.utils.model_helpers import table_exists


def validate_dataset(dataset_name):
    if not table_exists(dataset_name):
        raise ValidationError("Invalid table name: {}.".format(dataset_name))


def validate_many_datasets(list_of_datasets):
    for dataset in list_of_datasets:
        validate_dataset(dataset)


def validate_geom(geojson_str):
    try:
        return extract_first_geometry_fragment(geojson_str)
    except (ValueError, AttributeError):
        raise ValidationError("Invalid geom: {}".format(geojson_str))


def validate_network(network):
    if network.lower() not in NetworkMeta.index():
        raise ValidationError("Invalid network name: {}".format(network))


def validate_nodes(nodes):
    if isinstance(nodes, str):
        nodes = [nodes]
    valid_nodes = NodeMeta.index()
    for node in nodes:
        if node.lower() not in valid_nodes:
            raise ValidationError("Invalid node ID: {}".format(node))


def validate_features(features):
    if isinstance(features, str):
        features = [features]
    valid_features = FeatureMeta.index()
    for feature in features:
        feature = feature.split(".")[0].lower()
        if feature not in valid_features:
            raise ValidationError("Invalid feature of interest name: {}".format(feature))


def validate_sensors(sensors):
    if isinstance(sensors, str):
        sensors = [sensors]
    valid_sensors = SensorMeta.index()
    for sensor in sensors:
        sensor = sensor.lower()
        if sensor not in valid_sensors:
            raise ValidationError("Invalid sensor name: {}".format(sensor))


class Validator(Schema):
    """Base validator object using Marshmallow. Don't be intimidated! As scary
    as the following block of code looks it's quite simple, and saves us from
    writing validators. Let's break it down...

    <FIELD_NAME> = fields.<TYPE>(default=<DEFAULT_VALUE>, validate=<VALIDATOR FN>)

    The validator, when instantiated, has a method called 'dump'.which expects a
    dictionary of arguments, where keys correspond to <FIELD_NAME>. The validator
    has a default <TYPE> checker, that along with extra <VALIDATOR FN>s will
    accept or reject the value associated with the key. If the value is missing
    or rejected, the validator will substitute it with the value specified by
    <DEFAULT_VALUE>."""

    valid_aggs = {'day', 'week', 'month', 'quarter', 'year'}
    valid_formats = {'csv', 'geojson', 'json'}

    agg = fields.Str(default='week', validate=OneOf(valid_aggs))
    buffer = fields.Integer(default=100, validate=Range(0))
    dataset_name = fields.Str(default=None, validate=validate_dataset, dump_to='dataset')
    shape = fields.Str(default=None, validate=validate_dataset, dump_to='shapeset')
    dataset_name__in = fields.List(fields.Str(), validate=validate_many_datasets)
    date__time_of_day_ge = fields.Integer(default=0, validate=Range(0, 23))
    date__time_of_day_le = fields.Integer(default=23, validate=Range(0, 23))
    data_type = fields.Str(default='json', validate=OneOf(valid_formats))
    location_geom__within = fields.Str(default=None, dump_to='geom', validate=validate_geom)
    obs_date__ge = fields.DateTime(default=datetime.now() - timedelta(days=90))
    obs_date__le = fields.DateTime(default=datetime.now())
    limit = fields.Integer(default=1000, validate=Range(0, 10000))
    offset = fields.Integer(default=0, validate=Range(0))
    resolution = fields.Integer(default=500, validate=Range(0))
    job = fields.Bool(default=False)
    all = fields.Bool(default=False)


class DatasetRequiredValidator(Validator):
    """Some endpoints, like /detail-aggregate, should not be allowed to receive
    requests that do not specify a 'dataset_name' in the query string."""

    dataset_name = fields.Str(validate=validate_dataset, dump_to='dataset', required=True)


class NoGeoJSONValidator(Validator):
    """Some endpoints, like /timeseries, should not allow GeoJSON as a valid
    response format."""

    valid_formats = {'csv', 'json'}
    # Validator re-initialized so that it doesn't use old valid_formats.
    data_type = fields.Str(default='json', validate=OneOf(valid_formats))


class NoGeoJSONDatasetRequiredValidator(DatasetRequiredValidator):
    """Some endpoints, like /detail-aggregate, should not allow GeoJSON as a valid
    response format and require a dataset."""

    valid_formats = {'csv', 'json'}
    data_type = fields.Str(default='json', validate=OneOf(valid_formats))


class NoDefaultDatesValidator(Validator):
    """Some endpoints, specifically /datasets, will not return results with
    the original default dates (because the time window is so small)."""

    obs_date__ge = fields.Date(default=None)
    obs_date__le = fields.Date(default=None)


class ExportFormatsValidator(Validator):
    """For /shapes/<shapeset_name>?data_type=<format>"""

    valid_formats = {'shapefile', 'kml', 'json'}
    data_type = fields.Str(default='json', validate=OneOf(valid_formats))


class SensorNetworkValidator(Validator):
    network = fields.Str(allow_none=True, missing=None, default='array_of_things', validate=validate_network)
    nodes = fields.List(fields.Str(), default=None, missing=None, validate=validate_nodes)
    sensors = fields.List(fields.Str(), default=None, missing=None, validate=validate_sensors)
    feature = fields.Str(validate=validate_features)
    features = fields.List(fields.Str(), default=None, missing=None, validate=validate_features)

    geom = fields.Str(default=None, validate=validate_geom)
    start_datetime = fields.DateTime(default=lambda: datetime.utcnow() - timedelta(days=90))
    end_datetime = fields.DateTime(default=datetime.utcnow)
    filter = fields.Str(allow_none=True, missing=None, default=None)
    limit = fields.Integer(default=1000)
    offset = fields.Integer(default=0, validate=Range(0))


class NodeAggregateValidator(SensorNetworkValidator):
    valid_sensor_aggs = ("minute", "hour", "day", "week", "month", "year")

    node = fields.Str(required=True, validate=validate_nodes)
    features = fields.List(fields.Str(), required=True, validate=validate_features)
    function = fields.Str(missing="avg", default="avg", validate=lambda x: x.lower() in aggregate_fn_map)

    agg = fields.Str(default="hour", missing="hour", validate=lambda x: x in NodeAggregateValidator.valid_sensor_aggs)
    start_datetime = fields.DateTime(default=lambda: datetime.utcnow() - timedelta(days=1))


class RequiredFeatureValidator(SensorNetworkValidator):
    feature = fields.Str(validate=validate_features, required=True)


class DatadumpValidator(SensorNetworkValidator):
    start_datetime = fields.DateTime(default=lambda: datetime.utcnow() - timedelta(days=7))
    end_datetime = fields.DateTime(default=lambda: datetime.utcnow())
    limit = fields.Integer(default=None)


class IFTTTValidator(SensorNetworkValidator):
    start_datetime = fields.DateTime(default=lambda: datetime.utcnow() - timedelta(minutes=30))


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
    'agg': str,
    'buffer': int,
    'dataset': lambda x: MetaTable.get_by_dataset_name(x).point_table,
    'shapeset': lambda x: ShapeMetadata.get_by_dataset_name(x).shape_table,
    'data_type': str,
    'shape': lambda x: ShapeMetadata.get_by_dataset_name(x).shape_table,
    'dataset_name__in': lambda x: x.split(','),
    'date__time_of_day_ge': int,
    'date__time_of_day_le': int,
    'obs_date__ge': lambda x: parser.parse(x).date(),
    'obs_date__le': lambda x: parser.parse(x).date(),
    'date': lambda x: parser.parse(x).date(),
    'point_date': lambda x: parser.parse(x),
    'offset': int,
    'resolution': int,
    'geom': lambda x: make_fragment_str(extract_first_geometry_fragment(x)),
    'start_datetime': lambda x: x.isoformat().split('+')[0],
    'end_datetime': lambda x: x.isoformat().split('+')[0]
}


def convert(request_args):
    """Convert a dictionary of arguments from strings to their types. How the
    values are converted are specified by the converters dictionary defined
    above.

    :param request_args: dictionary of request arguments

    :returns: converted dictionary"""

    for key, value in list(request_args.items()):
        try:
            request_args[key] = converters[key](value)
        except (KeyError, TypeError, AttributeError, NoSuchTableError):
            # print "UNABLE TO CONVERT {} {}".format(key, value)
            pass
        except (DatabaseError, ProgrammingError):
            # Failed transactions, which we do expect, can cause
            # a DatabaseError with Postgres. Failing to rollback
            # prevents further queries from being carried out.
            postgres_session.rollback()


def marshmallow_validate(validator, args):
    # For validator dataset_name__in... need to find a better way to
    # make it play nice with the validator.
    if args.get('dataset_name__in'):
        args['dataset_name__in'] = args['dataset_name__in'].split(',')

    # This first validation step covers conditions that are dataset
    # agnostic. These are values can be used to apply to all datasets
    # (ex. obs_date), or concern the format of the response (ex. limit,
    # datatype, offset).

    # If there are errors, fail quickly and return.
    result = validator.load(args)
    if result.errors:
        return result

    # If all arguments are valid, fill in validator defaults.
    result = validator.dump(result.data)

    # Certain values will be dumped as strings. This conversion
    # makes them into their corresponding type. (ex. Table)
    convert(result.data)

    return result


def validate(validator, request_args):
    """Validate a dictionary of arguments. Substitute all missing fields with
    defaults if not explicitly told to do otherwise.

    :param validator: what kind of validator to use
    :param request_args: dictionary of arguments from a request object

    :returns: ValidatorResult namedtuple"""

    args = request_args.copy()

    result = marshmallow_validate(validator, args)

    # Holds messages concerning unnecessary parameters. These can be either
    # junk parameters, or redundant column parameters if a tree filter was
    # used.
    warnings = []

    # At this point validation splits. We can either validate tree-style column
    # arguments or validate them individually. We don't do both.

    # Determine unchecked parameters provided in the request.
    unchecked = set(args.keys()) - set(validator.fields.keys())

    # If tree filters were provided, ignore ALL unchecked parameters that are
    # not tree filters or response format information.
    if has_tree_filters(request_args):

        for key in request_args:
            value = args[key]
            if 'filter' in key:
                # This pattern matches the last occurrence of the '__' pattern.
                # Prevents an error that is caused by dataset names with trailing
                # underscores.
                t_name = re.split(r'__(?!_)', key)[0]

                # Report a filter which specifies a non-existent tree.
                try:
                    table = MetaTable.get_by_dataset_name(t_name).point_table
                except (AttributeError, NoSuchTableError):
                    try:
                        table = ShapeMetadata.get_by_dataset_name(t_name).shape_table
                    except (AttributeError, NoSuchTableError):
                        result.errors[t_name] = "Table name {} could not be found.".format(t_name)
                        return result

                # Report a tree which causes the JSON parser to fail.
                # Or a tree whose value is not valid.
                try:
                    cond_tree = json.loads(value)
                    if valid_tree(table, cond_tree):
                        result.data[key] = cond_tree
                except (ValueError, KeyError) as err:
                    result.errors[t_name] = "Bad tree: {} -- causes error {}.".format(value, err)
                    return result

            # These keys just have to do with the formatting of the JSON response.
            # We keep these values around even if they have no effect on a condition
            # tree.
            elif key in {'geom', 'offset', 'limit', 'agg', 'obs_date__le', 'obs_date__ge'}:
                pass

            # These keys are also ones that should be passed over when searching for
            # unused params. They are used, just in different forms later on, so no need
            # to report them.
            elif key in {'shape', 'dataset_name', 'dataset_name__in'}:
                pass

            # If the key is not a filter, and not used to format JSON, report
            # that we ignored it.
            else:
                warnings.append("Unused parameter {}, you cannot specify both "
                                "column and filter arguments.".format(key))

    # If no tree filters were provided, see if any of the unchecked parameters
    # are usable as column conditions.
    else:
        try:
            table = result.data['dataset']
        except KeyError:
            table = result.data.get('shapeset')
        for param in unchecked:
            field = param.split('__')[0]
            if table is not None:
                try:
                    value = args[param]

                    valid_column_condition(table, field, value)
                    result.data[param] = args[param]
                except KeyError:
                    warnings.append('Unused parameter value "{}={}"'.format(param, value))
                    warnings.append('{} is not a valid column for {}'.format(param, table))
                except ValueError:
                    warnings.append('Unused parameter value "{}={}"'.format(param, value))
                    warnings.append('{} is not a valid value for {}'.format(args[param], param))

    # ValidatorResult(dict, dict, list)
    return ValidatorResult(result.data, result.errors, warnings)


def sensor_network_validate(validator, request_args):
    """Validate a dictionary of arguments. Substitute all missing fields with
    defaults if not explicitly told to do otherwise.

    :param validator: type of validator to use
    :param request_args: dictionary of arguments from a request object

    :returns: ValidatorResult namedtuple"""

    args = request_args.copy()

    # # Prevent a time formatting issue that causes validator.load to act up
    # # The "+" sign in dates gets turned into a space character
    # if args.get("start_datetime"):
    #     args["start_datetime"] = args["start_datetime"].split(" ")[0]
    # if args.get("end_datetime"):
    #     args["end_datetime"] = args["end_datetime"].split(" ")[0]

    result = marshmallow_validate(validator, args)

    # fill in all nodes, sensors, and features within the network as a default
    # if this is not an observation query, KeyErrors will result
    try:
        network_name = result.data['network_name']
        if result.data['nodes'] is None:
            result.data['nodes'] = NodeMeta.index(network_name)
        if result.data['features'] is None:
            result.data['features'] = FeatureMeta.index(network_name)
        if result.data['sensors'] is None:
            result.data['sensors'] = SensorMeta.index(network_name)
    except KeyError:
        pass

    # Holds messages concerning unnecessary parameters. These can be either
    # junk parameters, or redundant column parameters if a tree filter was
    # used.
    warnings = []

    # Determine unchecked parameters provided in the request.
    unchecked = set(args.keys()) - set(validator.fields.keys())

    if 'filter' in list(args.keys()):
        raw_tree = result.data['filter']
        try:
            cond_tree = json.loads(raw_tree)
            t_name = result.data['feature']
            cond_tree['col'] = cond_tree['prop']
            del cond_tree['prop']
            table = reflect(t_name, MetaData(), redshift_engine)
            if valid_tree(table, cond_tree):
                result.data['filter'] = cond_tree
        except (ValueError, KeyError) as err:
            result.errors['filter'] = "Bad tree: {} -- causes error {}.".format(raw_tree, err)
            return result

    if unchecked:
        for param in unchecked:
            result.errors[param] = 'Not a valid filter'.format(param)

    # ValidatorResult(dict, dict, list)
    return ValidatorResult(result.data, result.errors, warnings)


def valid_tree(table, tree):
    """Given a dictionary containing a condition tree, validate all conditions
    nestled in the tree.

    :param table: table to build conditions for, need it for the columns
    :param tree: condition_tree

    :returns: boolean value, true if the tree is valid"""

    if not list(tree.keys()):
        raise ValueError("Empty or malformed tree.")

    op = tree.get('op')
    if not op:
        raise ValueError("Invalid keyword in {}".format(tree))

    if op == "and" or op == "or":
        return all([valid_tree(table, subtree) for subtree in tree['val']])

    elif op in field_ops:
        col = tree.get('col')
        val = tree.get('val')

        if col is None or val is None:
            err_msg = 'Missing or invalid keyword in {}'.format(tree)
            err_msg += ' -- use format "{\'op\': OP, \'col\': COL, \'val\', VAL}"'
            raise ValueError(err_msg)

        return valid_column_condition(table, col, val)

    else:
        raise ValueError("Invalid operation {}".format(op))


def valid_column_condition(table, column_name, value):
    """Establish whether or not a set of components is able to make a valid
    condition for the provided table.

    :param table: SQLAlchemy table object
    :param column_name: Name of the column
    :param value: target value"""

    try:
        if type(table) != sqlalchemy.sql.schema.Table:
            table = converters['dataset'](table)
        column = table.columns[column_name]
    except KeyError:
        raise KeyError("Invalid column name {}".format(column_name))

    # Dates are trickier than other types, the check that follows this will
    # not correctly approve string dates because they are usually coerced from
    # int values.
    if str(column.type) == 'DATE':
        try:
            parser.parse(value)
            return True
        except (AttributeError, TypeError):
            raise ValueError("Invalid value type for {}. Was expecting {}"
                             .format(value, 'datetime'))

    # If the value is not a date, we can do a couple more checks to see if the
    # value is usable. If the type of the value does not exactly match the
    # column type, try and coerce it to the column type. If it can't be coerced
    # it's not usable.
    try:
        if type(value) != column.type.python_type:
            column.type.python_type(value)  # Blessed Python.
    except (ValueError, TypeError):
        raise ValueError("Invalid value type for {}. Was expecting {}"
                         .format(value, column.type.python_type))

    return True


def has_tree_filters(request_args):
    """See if there are any <DATASET>__filter parameters.

    :param request_args: dictionary of request arguments
    :returns: boolean, true if there's a filter argument"""

    return any('filter' in key for key in list(request_args.keys()))
