from flask import Blueprint, request
from json import dumps, loads
from redis import Redis

from plenario.settings import REDIS_HOST_SAFE


blueprint = Blueprint("apiary", __name__)
redis = Redis(REDIS_HOST_SAFE)


@blueprint.route("/apiary/send_message", methods=["POST"])
def send_message():
    try:
        data = loads(request.data)
        redis.set(name="AOTMapper_" + data["name"], value=dumps(data["value"]))
    except (KeyError, ValueError):
        pass
