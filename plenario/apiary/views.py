from json import dumps, loads
from traceback import format_exc

from flask import Blueprint, make_response, request, flash, redirect
from redis import Redis
from sqlalchemy import select, desc

from plenario.auth import login_required
from plenario.database import redshift_base as rshift_base
from plenario.models.SensorNetwork import SensorMeta
from plenario.settings import REDIS_HOST
from plenario.tasks import unknown_features_resolve

blueprint = Blueprint("apiary", __name__)
redis = Redis(REDIS_HOST)


@blueprint.route("/apiary/send_message", methods=["POST"])
# @login_required
def send_message():
    try:
        data = loads(request.data.decode("utf-8"))
        if data["value"].upper() == "RESOLVE":
            unknown_features_resolve.delay(data["name"])
            print(("AOTMapper_" + data["name"]))
            redis.delete("AOTMapper_" + data["name"])
        else:
            redis.set(name="AOTMapper_" + data["name"], value=dumps(data["value"]))
        return make_response("Message received successfully!", 200)
    except (KeyError, ValueError):
        return make_response(format_exc(), 500)


@blueprint.route("/apiary/resolve/<sensor>")
@login_required
def resolve(sensor: str):
    task_id = unknown_features_resolve.delay(sensor).id
    message = 'Successfully queued resolve task for  {} (TASK ID: {})'
    message = message.format(sensor, task_id)
    flash(message)
    return redirect(request.referrer)


def index() -> list:
    """Generate the information necessary for displaying unknown features on the
    admin index page."""

    # todo: iterate over all networks

    rshift_base.metadata.reflect()
    unknown_features = rshift_base.metadata.tables['array_of_things_chicago__unknown_feature']

    query = select([unknown_features])               \
        .order_by(desc(unknown_features.c.datetime)) \
        .limit(5)
    rp = query.execute()

    results = []
    for row in rp:

        sensor = SensorMeta.query.get(row.sensor)

        if sensor is None:
            expected = "No metadata exists for this sensor!"
        else:
            expected = dumps(sensor.observed_properties, indent=2, sort_keys=True)

        result = {
            'sensor': row.sensor,
            'datetime': row.datetime,
            'incoming': dumps(loads(row.data), indent=2, sort_keys=True, default=str),
            'expected': expected
        }

        results.append(result)

    return results
