import re
from sqlalchemy import and_, or_, func


# field_ops
# =========
# Map codes we accept in API docs to SQLAlchemy function names

field_ops = {
    'gt': '__gt__',
    'ge': '__ge__',
    'lt': '__lt__',
    'le': '__le__',
    'ne': '__ne__',
    'like': 'like',
    'ilike': 'ilike',
    'is': 'is_',
    'isnot': 'isnot',
    'eq': 'eq',
    'in': 'in'
}


def parse_tree(table, condition_tree, literally=False):
    """Parse nested conditions provided as a dict for a single table. Wraps
    _parse_condition_tree and raises a ValueError if it fails.

    :param table: table object whose columns are being used in the conditions
    :param condition_tree: dictionary of conditions created from JSON
    :param literally: whether or not to create conditions as literal strings

    :returns SQLAlchemy conditions for querying the table with"""

    try:
        return _parse_condition_tree(table, condition_tree, literally)
    except Exception as ex:
        raise ValueError('{} caused parse to fail for table {} with args {}'
                         .format(ex, table, condition_tree))


def _parse_condition_tree(table, ctree, literally=False):
    """Parse nested conditions provided as a dict for a single table.

    :param table: table object whose columns are being used in the conditions
    :param ctree: dictionary of conditions created from JSON

    :returns SQLAlchemy conditions for querying the table with"""

    op = ctree['op']

    if op == "and":
        return and_(
            _parse_condition_tree(table, child)
            for child in ctree['val']
        )

    elif op == "or":
        return or_(
            _parse_condition_tree(table, child)
            for child in ctree['val']
        )

    elif op in field_ops:
        col = ctree['col']
        val = ctree['val']
        try:
            return _operator_to_condition(
                getattr(table, 'columns')[col], op, val, literally
            )

        # Exists for date__time_of_day. Since it doesn't come
        except KeyError:
            return getattr(col, field_ops[op])(val)


def parse_general(table, field, value):
    """To parse the non-tree style way of providing arguments.

    :param table: SQLAlchemy table objects
    :param field: specified column/table attribute
    :param value: target value to build a condition against

    :returns: table condition (SQL WHERE clause)"""

    # straigtforward time and geom filters
    if field in general_filters:
        return general_filters[field](table, value)

    field_tokens = field.split("__")
    column = table.columns.get(field_tokens[0])

    # equality filters based on table columns
    if len(field_tokens) == 1 and column is not None:
        return _operator_to_condition(column, 'eq', value)

    # comparison filters based on table columns
    elif len(field_tokens) == 2 and column is not None:
        op = field_tokens[1]

        if op in field_ops:
            return _operator_to_condition(column, op, value)


def _operator_to_condition(column, operator, operand, literally=False):
    """Convert an operation into a SQLAlchemy condition. Operators
    are mapped to SQLAlchemy methods with the field_ops dictionary.

    :param column: column object from the table to build condition for
    :param operator: string name of the desired operator
    :param operand: some target value or parameter
    :param literally: return condition as a string literal

    :returns: SQLAlchemy condition or string"""

    if operator == 'in':
        condition = column.in_(operand.split(','))
    elif operator == 'eq':
        condition = column == operand
    else:
        condition = getattr(column, field_ops[operator])(operand)

    if literally:
        operand = "'{}'".format(operand)
        condition = re.sub(r":\w*", operand, str(condition))

    return condition


# general_filters
# ===============
# Filters which apply to all the tables, but whose columns are not directly
# specified by an argument key. ex: 'obs_date__ge' translates into a condition
# that needs to be made for a table's 'point_date' column.

general_filters = {
    'obs_date__ge':
        lambda table, value:  table.c.point_date >= value,
    'obs_date__le':
        lambda table, value:  table.c.point_date <= value,
    'date__time_of_day_ge':
        lambda table, value:  date__time_of_day_filter(table, 'ge', value),
    'date__time_of_day_le':
        lambda table, value:  date__time_of_day_filter(table, 'le', value),
    'geom':
        lambda table, value:  table.c.geom.ST_Within(func.ST_GeomFromGeoJSON(value))
}


def date__time_of_day_filter(table, op, val):
    """Because I couldn't fit it into a one line lambda.

    :param table: SQLAlchemy table object
    :param op: string op code
    :param val: target value to be compared against

    :returns: table condition for just the hour"""

    column = func.date_part('hour', table.c.point_date)
    return getattr(column, field_ops[op])(val)
