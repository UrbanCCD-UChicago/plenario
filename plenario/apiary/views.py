from collections import defaultdict
from flask import Blueprint, make_response, request
from json import dumps, loads
from redis import Redis
from sqlalchemy import Table
from traceback import format_exc

from plenario.database import redshift_session as rshift_session
from plenario.database import session as psql_session
from plenario.database import Base as psql_base, app_engine as psql_engine
from plenario.database import redshift_Base as rshift_base
from plenario.database import redshift_engine as rshift_engine
from plenario.settings import REDIS_HOST

blueprint = Blueprint("apiary", __name__)
redis = Redis(REDIS_HOST)


def reflect(table_name, metadata, engine):
    """A helper method for reflecting tables into SQLAlchemy ORM objects.

    :param table_name: (string) target table
    :param metadata: (MetaData) SQLAlchemy container for database features
    :param engine: (Engine) SQLAlchemy object for executing db statements"""

    return Table(
        table_name,
        metadata,
        autoload=True,
        autoload_with=engine
    )


def map_unknown_to_foi(unknown, sensor_properties):
    """Given a valid unknown feature row, distribute the data stored within
    to the corresponding feature of interest tables by using the mapping
    defined by a sensor's observed properties.

    :param unknown: (object) a row returned from a SQLAlchemy query
    :param sensor_properties: (dict) holds mappings from node key to FOI"""

    # TODO: Make sure to handle errors, in case the resolved issue doesn't
    # TODO: actually fix what made this these observations misfits.

    foi_insert_vals = defaultdict(list)

    for key, value in list(loads(unknown.data).items()):
        foi = sensor_properties[key].split(".")[0]
        prop = sensor_properties[key].split(".")[1]
        foi_insert_vals[foi].append((prop, value))

    for foi, insert_vals in list(foi_insert_vals.items()):
        insert = "insert into {} (node_id, datetime, meta_id, sensor, {}) values ({})"
        columns = ", ".join(val[0] for val in insert_vals)

        values = "'{}', '{}', '{}', '{}', ".format(
            unknown.node_id,
            unknown.datetime,
            unknown.meta_id,
            unknown.sensor
        ) + ", ".join(repr(val[1]) for val in insert_vals)

        rshift_engine.execute(insert.format(foi, columns, values))

        delete = "delete from unknown_feature where node_id = '{}' and datetime = '{}' and meta_id = '{}' and sensor = '{}'"
        delete = delete.format(unknown.node_id, unknown.datetime, unknown.meta_id, unknown.sensor)

        rshift_engine.execute(delete)


def unknown_features_resolve(target_sensor):
    """When the issues for a sensor with an unknown error have been resolved,
    attempt to recover sensor readings that were originally suspended in the
    unknowns table and insert them into the correct feature of interest table.

    :param target_sensor: (str) resolved sensor"""

    sensors = reflect("sensor__sensors", psql_base.metadata, psql_engine)
    unknowns = reflect("unknown_feature", rshift_base.metadata, rshift_engine)

    # Grab the set of keys that are used to assert if an unkown is correct
    c_obs_props = sensors.c.observed_properties
    c_name = sensors.c.name

    q = psql_session.query(c_obs_props)
    sensor_properties = q.filter(c_name == target_sensor).first()[0]

    # Grab all the candidate unknown observations
    c_sensor = unknowns.c.sensor
    target_unknowns = rshift_session.query(unknowns).filter(c_sensor == target_sensor)
    target_unknowns = target_unknowns.all()

    for unknown in target_unknowns:
        unknown_data = loads(unknown.data)
        if not all(key in list(sensor_properties.keys()) for key in list(unknown_data.keys())):
            continue
        map_unknown_to_foi(unknown, sensor_properties)


@blueprint.route("/apiary/send_message", methods=["POST"])
# @login_required
def send_message():
    try:
        data = loads(request.data.decode("utf-8"))
        if data["value"].upper() == "RESOLVE":
            unknown_features_resolve(data["name"])
            print(("AOTMapper_" + data["name"]))
            redis.delete("AOTMapper_" + data["name"])
        else:
            redis.set(name="AOTMapper_" + data["name"], value=dumps(data["value"]))
        return make_response("Message received successfully!", 200)
    except (KeyError, ValueError):
        return make_response(format_exc(), 500)
