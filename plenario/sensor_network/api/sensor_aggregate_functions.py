import pdb

from collections import defaultdict
from datetime import timedelta

from dateutil.parser import parse as date_parse
from sqlalchemy import and_, func, Table
from sqlalchemy.sql import select

from plenario.database import redshift_Base as RBase, redshift_session as r_session
from plenario.database import session, redshift_engine as r_engine
from plenario.sensor_network.sensor_models import NodeMeta


def _reflect(table_name, metadata, engine):
    """Helper function for an oft repeated block of code.

    :param table_name: (str) table name
    :param metadata: (MetaData) SQLAlchemy object found in a declarative base
    :param engine: (Engine) SQLAlchemy object to send queries to the database
    :returns: (Table) SQLAlchemy object"""

    print table_name

    return Table(
        table_name,
        metadata,
        autoload=True,
        autoload_with=engine
    )


def _valid_columns(node, target_sensors, target_feature_properties):
    """Retrieve the set of valid feature properties to return given feature and
    sensor filters.

    :param node: (str) node id
    :param target_sensors: (list) containing sensor ids
    :param target_feature_properties: (dict) map of target FOI properties
    :returns: (set) column keys to be used in the aggregate query"""

    select_node_meta = session.query(NodeMeta).filter(NodeMeta.id == node)
    target_node = select_node_meta.first()
    sensors = target_node.sensors

    columns = set()
    for sensor in sensors:
        if target_sensors:
            if sensor.name not in target_sensors:
                continue
        for val in sensor.observed_properties.values():
            current_feature = val.split(".")[0]
            current_property = val.split(".")[1]
            if current_feature not in target_feature_properties:
                continue
            # We will only check against properties if properties were specified
            # ex. magnetic_field.x, magnetic_field.y ...
            if target_feature_properties[current_feature]:
                if current_property not in target_feature_properties[current_feature]:
                    continue
            columns.add(val.split(".")[1].lower())

    return columns


def _zero_out_datetime(dt, unit):
    """To fix a super obnoxious issue where datetrunc (or SQLAlchemy) would
    break up resulting values if provided a datetime with nonzero values more
    granular than datetrunc expects. Ex. calling datetrunc("hour", ...) with
    a datetime such as 2016-09-20 08:12:12.

    :param dt: (datetime) to zero out
    :param unit: (str) from what unit of granularity do we zero
    :returns: (datetime) a well-behaved, non query-breaking datetime"""

    units = ["year", "month", "day", "hour", "minute", "second", "microsecond"]
    i = units.index(unit) + 1
    for zeroing_unit in units[i:]:
        dt = dt.replace(**{zeroing_unit: 0})
    return dt


def aggregate(args, agg_label, agg_fn, buckets, agg_unit):

    expected = ("node_id", "feature", "start_datetime", "sensors")
    node, feature, start_datetime, sensors = (args.data.get(k) for k in expected)

    # To prevent a two-day consuming, sanity-draining bug
    start_datetime = date_parse(start_datetime)
    start_datetime = _zero_out_datetime(start_datetime, agg_unit)

    # Break up comma-delimited query arguments
    target_features = feature.split(",")
    target_sensors = sensors.split(",") if sensors else None

    # Generate a map of the target features and properties
    target_feature_properties = dict()
    for feature in target_features:
        try:
            feature, f_property = feature.split(".")
            target_feature_properties.setdefault(feature, []).append(f_property)
        except ValueError:
            target_feature_properties[feature] = None

    valid_columns = _valid_columns(node, target_sensors, target_feature_properties)
    if not valid_columns:
        raise ValueError("Your query returns no results.")

    # Get delimiting datetimes to break the records up with
    datetimes = list()
    for i in range(0, buckets + 1):
        # The + "s" is required, timedelta takes plural version of the word
        datetimes.append(start_datetime + timedelta(**{str(agg_unit) + "s": i}))

    for dt in datetimes:
        print "aggregate.datetimes.dt: {}".format(dt)

    obs_table = _reflect(feature.split(".")[0], RBase.metadata, r_engine)

    select_aggregates = [
        func.date_trunc("hour", obs_table.c.datetime).label("time_bucket"),
    ]

    meta_columns = ("node_id", "datetime", "meta_id", "sensor")
    for col in obs_table.c:
        if col.name in meta_columns:
            continue
        if col.name not in valid_columns:
            continue
        if str(col.type).split("(")[0] != "DOUBLE PRECISION":
            continue
        select_aggregates.append(agg_fn(col).label(col.name))
        select_aggregates.append(func.count(col).label(col.name + "_count"))

    aggregates = list()
    for i in xrange(0, buckets + 1):
        if i == buckets:
            continue
        query = select(select_aggregates).where(and_(
            obs_table.c.datetime >= datetimes[i],
            obs_table.c.datetime <= datetimes[i + 1]
        )).group_by("time_bucket")
        payload = r_session.execute(query).fetchall()
        if payload:
            aggregates += payload
        else:
            # Drop the microseconds for display
            aggregates += [{"time_bucket": datetimes[i].isoformat().split("+")[0], "count": 0}]

    results = list()
    for agg in aggregates:
        aggregate_json = defaultdict(dict)

        for key in agg.keys():
            if key == "time_bucket":
                aggregate_json["time_bucket"] = agg[key]
            elif key == "count":
                aggregate_json["count"] = agg[key]
            elif "count" in key:
                aggregate_json[key.split("_")[0]]["count"] = agg[key]
            else:
                aggregate_json[key][agg_label] = agg[key]

        results.append(aggregate_json)

    return results


def aggregate_averages(args):

    return aggregate(
        args=args,
        agg_label="avg",
        agg_fn=func.avg,
        buckets=args.data.get("buckets"),
        agg_unit=args.data.get("agg_unit")
    )


def aggregate_stdevs(args):

    return aggregate(
        args=args,
        agg_label="std",
        agg_fn=func.stddev,
        buckets=args.data.get("buckets"),
        agg_unit=args.data.get("agg_unit")
    )


def aggregate_variances(args):

    return aggregate(
        args=args,
        agg_label="var",
        agg_fn=func.variance,
        buckets=args.data.get("buckets"),
        agg_unit=args.data.get("agg_unit")
    )


aggregate_fn_map = {
    "avg": aggregate_averages,
    "std": aggregate_stdevs,
    "var": aggregate_variances
}
