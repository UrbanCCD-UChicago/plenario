from collections import defaultdict
from datetime import timedelta
from dateutil.parser import parse as date_parse
from sqlalchemy import and_, func, Table
from sqlalchemy.sql import select

from plenario.database import redshift_Base, app_engine as p_engine
from plenario.database import redshift_engine as r_engine
from plenario.sensor_network.sensor_models import NodeMeta


# TODO: Take this formula and make it generalizable to any aggregate
# TODO: function.
def aggregate_averages(args):

    expected = ("node_id", "feature", "start_datetime")
    node, feature, start_datetime = (args.data.get(k) for k in expected)
    start_datetime = date_parse(start_datetime)

    # Get valid feature properties that this node reports on
    select_node_meta = select([NodeMeta]).where(NodeMeta.id == node)
    target_node = p_engine.execute(select_node_meta).first()
    target_sensors = target_node.sensors

    # A single node can have multiple sensors that map to the same FOI!
    valid_columns = set([
        sensor.observed_properties.keys() for sensor in target_sensors
    ])

    # Sets how many buckets are created
    start = 0
    stop = 7

    # Reflect the target feature table
    obs_table = Table(
        feature,
        redshift_Base.metadata,
        autoload=True,
        autoload_with=r_engine
    )

    # Get delimiting datetimes to break the records up with
    datetimes = list()
    for i in xrange(0, stop):
        datetimes.append(start_datetime + timedelta(hours=i))

    select_averages = [
        func.date_trunc("hour", obs_table.c.datetime).label("time_bucket"),
    ]

    meta_columns = ("node_id", "datetime", "meta_id", "sensor")
    for col in obs_table.c:
        if col.name in meta_columns:
            continue
        if col.name not in valid_columns:
            continue
        # if col.type != "double precision":
        #     continue
        select_averages.append(func.avg(col).label(col.name))
        select_averages.append(func.count(col).label(col.name + "_count"))

    aggregates = list()
    for i in xrange(start, stop):
        # Prevent IndexError
        if i == stop - 1:
            continue
        raw_records = r_engine.execute(
            select(select_averages).where(and_(
                obs_table.c.datetime >= datetimes[i],
                obs_table.c.datetime <= datetimes[i + 1]
            )).group_by("time_bucket")
        ).fetchall()

        # TODO: Eventually processing will be done before this append
        aggregates += raw_records

    results = list()
    for aggregate in aggregates:
        aggregate_json = defaultdict(dict)
        aggregate_json["time_bucket"] = None

        for key in aggregate.keys():
            if key in aggregate_json.keys():
                aggregate_json[key] = aggregate[key]
            elif "count" in key:
                aggregate_json[key.split("_")[0]]["count"] = aggregate[key]
            else:
                aggregate_json[key]["avg"] = aggregate[key]

        results.append(aggregate_json)

    return results
