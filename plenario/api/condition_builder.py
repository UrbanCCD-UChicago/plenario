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
            _parse_condition_tree(table, child, literally)
            for child in ctree['val']
        )

    elif op == "or":
        return or_(
            _parse_condition_tree(table, child, literally)
            for child in ctree['val']
        )

    elif op in field_ops:
        col = ctree['col']
        val = ctree['val']
        try:
            return _operator_to_condition(
                table.columns[col], op, val, literally
            )

        # Exists for date__time_of_day. Since it doesn't come as a string
        # that could specify a column, but rather as a column-like object
        # itself.
        except KeyError:
            return getattr(col, field_ops[op])(val)


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
        # Normally, SQLAlchemy would construct a condition with placeholder
        # values that would be filled in by the underlying database driver.
        #
        # example_condition = some_column >= :some_column_placeholder
        #
        # The following code takes that condition, and literallizes it
        # to a string, by filling in all the placeholder values.

        # Wraps the value in single quotes, this will only work for PostgreSQL.
        operand = "'{}'".format(operand)
        # Substitues the :params with the actual values.
        condition = re.sub(r":\w*", operand, str(condition))

    return condition
