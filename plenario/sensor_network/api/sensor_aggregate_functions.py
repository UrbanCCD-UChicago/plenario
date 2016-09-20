from collections import defaultdict
from datetime import timedelta

from dateutil.parser import parse as date_parse
from sqlalchemy import and_, func, Table
from sqlalchemy.sql import select

from plenario.database import redshift_Base as RBase
from plenario.database import session, redshift_engine as r_engine
from plenario.sensor_network.sensor_models import NodeMeta


def _reflect(table_name, metadata, engine):

    return Table(table_name, metadata, autoload=True, autoload_with=engine)


def aggregate(args, agg_label, agg_fn, buckets):

    node = args.data.get("node_id")
    feature = args.data.get("feature")
    start_datetime = date_parse(args.data.get("start_datetime"))

    # Reflect up the necessary tables
    obs_table = _reflect(feature, RBase.metadata, r_engine)

    target_node = session.query(NodeMeta).filter(NodeMeta.id == node).first()
    target_sensors = target_node.sensors

    # Find out which subset of the possible observed properties are valid
    valid_columns = set()
    for sensor in target_sensors:
        for val in sensor.observed_properties.values():
            valid_columns.add(val.split(".", 1)[1].lower())

    # Get delimiting datetimes to break the records up with
    datetimes = list()
    for i in xrange(0, buckets + 1):
        datetimes.append(start_datetime + timedelta(hours=i))

    select_averages = [
        func.date_trunc("hour", obs_table.c.datetime).label("time_bucket"),
    ]

    meta_columns = ("node_id", "datetime", "meta_id", "sensor")
    for col in obs_table.c:
        if col.name.lower() in meta_columns:
            continue
        if col.name.lower() not in valid_columns:
            continue
        # if col.type != "double precision":
        #     continue
        select_averages.append(agg_fn(col).label(col.name))
        select_averages.append(func.count(col).label(col.name + "_count"))

    aggregates = list()
    for i in xrange(0, buckets + 1):
        # Prevent IndexError
        if i == buckets:
            continue
        payload = r_engine.execute(
            select(select_averages).where(and_(
                obs_table.c.datetime >= datetimes[i],
                obs_table.c.datetime <= datetimes[i + 1]
            )).group_by("time_bucket")
        ).fetchall()

        aggregates += payload

    results = list()
    for agg in aggregates:
        aggregate_json = defaultdict(dict)
        aggregate_json["time_bucket"] = None

        for key in agg.keys():
            if key in aggregate_json.keys():
                aggregate_json[key] = agg[key]
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
        buckets=6
    )


def aggregate_stdevs(args):

    return aggregate(
        args=args,
        agg_label="std",
        agg_fn=func.stddev,
        buckets=6
    )


def aggregate_variances(args):

    return aggregate(
        args=args,
        agg_label="var",
        agg_fn=func.variance,
        buckets=6
    )


aggregate_fn_map = {
    "avg": aggregate_averages,
    "std": aggregate_stdevs,
    "var": aggregate_variances
}
