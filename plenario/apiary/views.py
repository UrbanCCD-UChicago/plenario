from collections import defaultdict
from json import dumps, loads
from traceback import format_exc

from flask import Blueprint, make_response, request
from redis import Redis

from plenario.auth import login_required
from plenario.settings import REDIS_HOST_SAFE

blueprint = Blueprint("apiary", __name__)
redis = Redis(REDIS_HOST_SAFE)


# @login_required
@blueprint.route("/apiary/send_message", methods=["POST"])
def send_message():
    try:
        data = loads(request.data)
        redis.set(name="AOTMapper_" + data["name"], value=dumps(data["value"]))
        return make_response("Message received successfully!", 200)
    except (KeyError, ValueError):
        return make_response(format_exc(), 500)


@login_required
@blueprint.route("/apiary/mapper_errors", methods=["GET"])
def mapper_errors():
    errors = defaultdict(list)
    for key in redis.scan_iter(match="AOTMapper_*"):
        errors[key].append(redis.get(key))
    return make_response(dumps(errors), 200)
