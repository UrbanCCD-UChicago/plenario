import json
import uuid
import time

from dateutil.parser import parse
from flask import request, make_response

from plenario.api.common import crossdomain
from plenario.api.common import unknown_object_json_handler
from plenario.api.validator import sensor_network_validate, RequiredFeatureValidator
from plenario.sensor_network.api.sensor_networks import sanitize_validated_args, get_observation_queries
from plenario.api.response import bad_request

# dictionary mapping the curated drop-down list name to the correct feature and property
curated_map = {"Temperature": "temperature.temperature",
               "Humidity": "relative_humidity.humidity"}


# TODO: format errors how ifttt wants
# TODO: decide on time or datetime
@crossdomain(origin="*")
def get_ifttt():
    input_args = request.json
    args = dict()
    args['network'] = 'array_of_things'
    args['nodes'] = [input_args['triggerFields']['node']]
    args['feature'] = curated_map[input_args['triggerFields']['prop']].split('.')[0]
    args['limit'] = input_args['limit'] if input_args['limit'] else 50
    args['filter'] = {'prop': curated_map[input_args['triggerFields']['prop']].split('.')[1],
                      'op': input_args['triggerFields']['op'],
                      'val': input_args['triggerFields']['val']}
    # include the curated input property so we can return it to the user for display purposes
    args['property'] = input_args['triggerFields']['prop']

    fields = ('network', 'nodes', 'feature', 'sensors', 'limit', 'filter')

    validated_args = sensor_network_validate(RequiredFeatureValidator(only=fields), args)
    if validated_args.errors:
        return bad_request(validated_args.errors)
    validated_args.data.update({
        "features": [validated_args.data["feature"]],
        "feature": None
    })
    validated_args = sanitize_validated_args(validated_args)

    observation_queries = get_observation_queries(validated_args)
    if type(observation_queries) != list:
        return observation_queries

    return run_ifttt_queries(validated_args, observation_queries)


def format_ifttt(obs, prop):
    obs_response = {
        "node": obs.node_id,
        "datetime": obs.datetime.isoformat().split('+')[0],
        "property": prop,
        "value": getattr(obs, prop),
        "meta": {
            "id": uuid.uuid1().hex,
            "timestamp": int(time.time())
        }
    }

    return obs_response


def run_ifttt_queries(args, queries):
    data = list()
    for query, table in queries:
        data += [format_ifttt(obs, args['property']) for obs in query.all()]

    data.sort(key=lambda x: parse(x["datetime"]), reverse=True)

    resp = {
        "data": data
    }
    resp = make_response(json.dumps(resp, default=unknown_object_json_handler), 200)
    resp.headers['Content-Type'] = 'application/json'
    return resp
