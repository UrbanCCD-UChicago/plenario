from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timedelta

from sqlalchemy import Table, and_, asc, func
from sqlalchemy.sql import select

from plenario.database import redshift_base as redshift_base, redshift_session as r_session


def _fill_in_blanks(aggregates, agg_unit, start_dt, end_dt):
    """Given a list of row proxies, interpolate empty results where the gap
    between the datetime of two results exceeds the agg_unit.

    :param aggregates: (SQLAlchemy) row proxy objects
    :param agg_unit: (str) unit of time
    :returns: (list) of row proxies and row proxy-likes
    """
    start_dt = _zero_out_datetime(start_dt, agg_unit)
    end_dt = _zero_out_datetime(end_dt, agg_unit)
    start_dt = start_dt.replace(tzinfo=None)
    end_dt = end_dt.replace(tzinfo=None)

    if not aggregates:
        return aggregates

    placeholder = _generate_placeholder(aggregates[0])

    if start_dt < aggregates[0]['time_bucket']:
        placeholder['time_bucket'] = start_dt
        aggregates.insert(0, deepcopy(placeholder))
    if end_dt > aggregates[-1]['time_bucket']:
        placeholder['time_bucket'] = end_dt
        aggregates.append(deepcopy(placeholder))

    filled_out_aggs = list()

    for i, agg in enumerate(aggregates):
        if i == len(aggregates) - 1:
            continue

        next_agg_time = aggregates[i + 1]['time_bucket']
        candidate_time = agg['time_bucket'] + timedelta(**{agg_unit + 's': 1})
        while next_agg_time != candidate_time:
            placeholder['time_bucket'] = candidate_time
            filled_out_aggs.append(deepcopy(placeholder))
            candidate_time += timedelta(**{agg_unit + 's': 1})
        filled_out_aggs.append(agg)

    return sorted(filled_out_aggs, key=lambda x: x['time_bucket'])


def _format_aggregates(aggregates, agg_label, agg_unit, start_dt, end_dt):
    """Given a list of row proxies, format them into serveable JSON.

    :param aggregates: (SQLAlchemy) row proxy objects (imitateable with dicts)
    :param agg_label: (str) name of the aggregate function used
    :returns: (list) of dictionary objects that can be dumped to JSON
    """
    results = list()
    for agg in aggregates:
        aggregate_json = defaultdict(dict)

        for key in list(agg.keys()):
            if key == 'time_bucket':
                aggregate_json['time_bucket'] = agg[key]
            elif key == 'count':
                aggregate_json['count'] = agg[key]
            elif 'count' in key:
                aggregate_json[key.rsplit('_', 1)[0]]['count'] = agg[key]
            else:
                aggregate_json[key][agg_label] = agg[key]

        results.append(aggregate_json)

    return _fill_in_blanks(results, agg_unit, start_dt, end_dt)


def _generate_aggregate_selects(table, target_columns, agg_fn, agg_unit):
    """Return the select statements used to generate a time bucket and apply
    aggregation to each target column.

    :param table: (SQLAlchemy) reflected table object
    :param target_columns: (list) contains strings
    :param agg_fn: (function) compiles to a prepared statement
    :param agg_unit: (str) used by date_trunc to generate time buckets
    :returns: (list) containing SQLAlchemy prepared statements
    """
    selects = [func.date_trunc(agg_unit, table.c.datetime).label('time_bucket')]

    meta_columns = ('node_id', 'datetime', 'meta_id', 'sensor')
    for col in table.c:
        if col.name in meta_columns:
            continue
        if col.name not in target_columns:
            continue
        if str(col.type).split('(')[0] != 'DOUBLE PRECISION':
            continue
        selects.append(agg_fn(col).label(col.name))
        selects.append(func.count(col).label(col.name + '_count'))

    return selects


def _generate_placeholder(aggreagte):
    """Generate a placeholder dictionary for filling in empty timebuckets.

    :param aggreagte: (dict) created from row proxy info
    :returns: (dict) placeholder copy
    """
    placeholder = deepcopy(aggreagte)
    for key, value in list(placeholder.items()):
        if key == 'time_bucket':
            continue
        elif key == 'count':
            placeholder[key] = 0
        elif type(value) == dict:
            placeholder[key] = _generate_placeholder(value)
        else:
            placeholder[key] = None
    return placeholder


def _reflect(table_name, metadata, engine):
    """Helper function for an oft repeated block of code.

    :param table_name: (str) table name
    :param metadata: (MetaData) SQLAlchemy object found in a declarative base
    :param engine: (Engine) SQLAlchemy object to send queries to the database
    :returns: (Table) SQLAlchemy object
    """
    return Table(
        table_name,
        metadata,
        autoload=True,
        autoload_with=engine
    )


# TODO(heyzoos)
# This could be replaced with a generalized validator for sensor network trees.
def _valid_columns(node, target_sensors, target_features, target_properties=None):
    """Retrieve the set of valid feature properties to return, given feature and sensor filters.
    """
    sensors = node.sensors

    columns = set()
    for sensor in sensors:
        if target_sensors:
            if sensor.name not in target_sensors:
                continue
        for val in list(sensor.observed_properties.values()):
            current_feature = val.split('.')[0]
            current_property = val.split('.')[1]
            if current_feature not in target_features:
                continue
            if target_properties and current_property not in target_properties:
                continue
            columns.add(val.split('.')[1].lower())

    return columns


def _zero_out_datetime(dt, unit):
    """To fix a super obnoxious issue where datetrunc (or SQLAlchemy) would
    break up resulting values if provided a datetime with nonzero values more
    granular than datetrunc expects. Ex. calling datetrunc('hour', ...) with
    a datetime such as 2016-09-20 08:12:12.

    Note that if any unit greater than an hour is provided, this method will
    zero hours and below, nothing more.

    :param dt: (datetime) to zero out
    :param unit: (str) from what unit of granularity do we zero
    :returns: (datetime) a well-behaved, non query-breaking datetime
    """
    units = ['year', 'month', 'day', 'hour', 'minute', 'second', 'microsecond']
    i = units.index(unit) + 1
    for zeroing_unit in units[i:]:
        try:
            dt = dt.replace(**{zeroing_unit: 0})
        except ValueError:
            pass
    return dt


def aggregate(args, agg_label, agg_fn):
    """Generate aggregates on node features of interest organized into chunks
    of time.

    :param args: (ValidatorResult) validated user parameters
    :param agg_label: (str) name of the aggregate function being used
    :param agg_fn: (function) aggregate function that is being applied
    :returns: (list) of dictionary objects that can be dumped to JSON
    """
    network = args['network']
    agg_unit = args['agg']
    node = args['node']
    feature = args['feature']
    sensors = args.get('sensors')
    property_ = args.get('property')

    start_dt = datetime.now() - timedelta(days=7) \
        if args.get('start_datetime') is None \
        else args['start_datetime']

    end_dt = datetime.now() \
        if args.get('end_datetime') is None \
        else args['end_datetime']

    target_sensors = [sensor.name for sensor in sensors]

    # Generate a map of the target features and properties
    target_features = [feature.name]
    target_properties = [p['name'] for p in feature.observed_properties]
    if property_ and property_ in target_properties:
        target_properties = [property_]

    # Determine which columns, if any, can be aggregated from the target node
    valid_columns = _valid_columns(node, target_sensors, target_features, target_properties)

    if not valid_columns:
        raise ValueError('Your query returns no results. You have specified '
                         'filters which are likely contradictory (for example '
                         'filtering on a sensor which doesn\'t have the feature '
                         'you are aggregating for)')

    redshift_base.metadata.reflect()
    obs_table = redshift_base.metadata.tables[network.name + '__' + feature.name]

    # Generate the necessary select statements and datetime delimiters
    selects = _generate_aggregate_selects(obs_table,
                                          valid_columns,
                                          agg_fn,
                                          agg_unit)

    # Generate the base query
    query = select(selects).where(and_(
        obs_table.c.datetime >= start_dt,
        obs_table.c.datetime < end_dt,
        obs_table.c.node_id == node.id
    ))

    # If sensors are specified, filter on them
    if target_sensors:
        query = query.where(obs_table.c.sensor.in_(target_sensors))

    # Format the query
    query = query.group_by('time_bucket').order_by(asc('time_bucket'))

    # Execute and format the result
    results = r_session.execute(query).fetchall()
    return _format_aggregates(results, agg_label, agg_unit, start_dt, end_dt)


aggregate_fn_map = {
    'avg': lambda args: aggregate(args, 'avg', func.avg),
    'std': lambda args: aggregate(args, 'std', func.stddev),
    'var': lambda args: aggregate(args, 'var', func.variance),
    'max': lambda args: aggregate(args, 'max', func.max),
    'min': lambda args: aggregate(args, 'min', func.min),
    'med': lambda args: aggregate(args, 'med', func.median),
}
