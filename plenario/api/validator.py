import json

from collections import namedtuple
from datetime import datetime, timedelta
from dateutil import parser
from marshmallow import fields, Schema
from marshmallow.validate import Range, Length, OneOf
from sqlalchemy.exc import DatabaseError, ProgrammingError, NoSuchTableError

from plenario.api.common import extract_first_geometry_fragment, make_fragment_str
from plenario.api.condition_builder import field_ops
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
    shapeset_name = fields.Str(default=None, validate=OneOf(ShapeMetadata.tablenames()), dump_to='shapeset')
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
    :param request_args: dictionary of arguments from a request object

    :returns: ValidatorResult namedtuple"""

    args = request_args.copy()

    # This first validation step covers conditions that are dataset
    # agnostic. These are values can be used to apply to all datasets
    # (ex. obs_date), or concern the format of the response (ex. limit,
    # datatype, offset).

    # Convert arguments from strings to corresponding types to work with
    # the marshmallow validator.
    convert(args)
    # Get the validation results, default values are substituted
    # for arguments whose values were missing or invalid.
    result = validator.dump(args)
    # Certain values will be dumped as strings. This extra conversion
    # returns them to their original type. (ex. DateTime, Table)
    convert(result.data)

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
                t_name = key.split('__')[0]

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
                except ValueError as err:
                    result.errors[t_name] = "Tree {} causes error {}.".format(value, err)
                    return result

            # These keys just have to do with the formatting of the JSON response.
            # We keep these values around even if they have no effect on a condition
            # tree.
            elif key in {'geom', 'offset', 'limit', 'agg', 'obs_date__le', 'obs_date__ge'}:
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
                    valid_column_condition(table, field, args[param])
                    result.data[param] = args[param]
                except ValueError as err:
                    warnings.append('Unused parameter value "{}={}"'.format(param, args[param]))
                    warnings.append('{} is not a valid column for {}'.format(param, table))
            else:
                warnings.append('Unused parameter value "{}={}"'.format(param, args[param]))
                warnings.append('No table provided for {}'.format(param))

    # ValidatorResult(dict, dict, list)
    return ValidatorResult(result.data, result.errors, warnings)


def valid_tree(table, tree):
    """Given a dictionary containing a condition tree, validate all conditions
    nestled in the tree.

    :param table: table to build conditions for, need it for the columns
    :param tree: condition_tree

    :returns: boolean value, true if the tree is valid"""

    if not tree.keys():
        raise ValueError("Empty or malformed tree.")

    op = tree['op']

    if op == "and" or op == "or":
        return all([valid_tree(table, subtree) for subtree in tree['val']])

    elif op in field_ops:
        col = tree['col']
        val = tree['val']
        return valid_column_condition(table, col, val)


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

    try:
        column = table.columns[column_name]
    except KeyError:
        raise ValueError("Invalid column name {}".format(column_name))

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

    return any('filter' in key for key in request_args.keys())
