import json
import uuid
import time

from dateutil.parser import parse
from flask import request, make_response
from os import environ

from plenario.api.common import crossdomain
from plenario.api.common import unknown_object_json_handler
from plenario.api.validator import sensor_network_validate, IFTTTValidator
from plenario.sensor_network.api.sensor_networks import sanitize_validated_args, get_observation_queries, \
    get_raw_metadata
from plenario.api.response import bad_request

# dictionary mapping the curated drop-down list name to the correct feature and property
curated_map = {"temperature": "temperature.temperature",
               "relative humidity": "relative_humidity.humidity",
               "light intensity": "light_intensity.640nm",
               "sound level": "sound.instantaneous_sample",
               "atmospheric pressure": "atmospheric_pressure.pressure",
               "SO2 concerntration": "gas_concerntration.so2",
               "H2S concerntration": "gas_concerntration.h2s",
               "NO2 concerntration": "gas_concerntration.no2",
               "O3 concerntration": "gas_concerntration.o3",
               "CO concerntration": "gas_concerntration.co",
               "2.5 micron particulate matter": "particulate_matter.2p5",
               "10 micron particulate matter": "particulate_matter.10"}


@crossdomain(origin="*")
def get_ifttt_observations():
    """Performs the query detailed by the IFTTT user's
       input arguments and returns the formatted response.
       Essentially acts as a shim to /query?filter
       to satisfy IFTTT request and return format.

       :endpoint: /ifttt/v1/triggers/property_comparison
       :returns: (json) response"""

    if request.headers.get('IFTTT-Channel-Key') != environ.get('IFTTT_CHANNEL_KEY'):
        return make_ifttt_error("incorrect channel key", 401)

    input_args = request.json
    args = dict()
    try:
        args['network'] = 'plenario_development'  # TODO: change to array_of_things when deployed
        args['nodes'] = [input_args['triggerFields']['node']]
        args['feature'] = curated_map[input_args['triggerFields']['curated_property']].split('.')[0]
        args['limit'] = input_args['limit'] if 'limit' in input_args.keys() else 50
        args['filter'] = json.dumps({'prop': curated_map[input_args['triggerFields']['curated_property']].split('.')[1],
                                     'op': input_args['triggerFields']['op'],
                                     'val': float(input_args['triggerFields']['val'])})
        # pass through the curated input property so we can return it to the user for display purposes
        curated_property = input_args['triggerFields']['curated_property']
    except (KeyError, ValueError) as err:
        return make_ifttt_error(str(err), 400)

    # override the normal limit 0 behaviour, which is to apply no limit
    if args['limit'] == 0:
        return make_ifttt_response([])

    fields = ('network', 'nodes', 'feature', 'sensors',
              'start_datetime', 'end_datetime', 'limit', 'filter')

    validated_args = sensor_network_validate(IFTTTValidator(only=fields), args)
    if validated_args.errors:
        return make_ifttt_error(validated_args.errors, 400)
    validated_args.data.update({
        "features": [validated_args.data["feature"]],
        "feature": None
    })
    validated_args = sanitize_validated_args(validated_args)

    observation_queries = get_observation_queries(validated_args)
    if type(observation_queries) != list:
        return observation_queries

    return run_ifttt_queries(observation_queries, curated_property)


@crossdomain(origin="*")
def get_ifttt_meta(field):
    """Returns a list of valid drop-down options for node and property.

       :endpoint: /ifttt/v1/triggers/property_comparison/fields/<field>/options
       :param field: (string) type of metadata to return
       :returns: (json) response"""

    if request.headers.get('IFTTT-Channel-Key') != environ.get('IFTTT_CHANNEL_KEY'):
        return make_ifttt_error("incorrect channel key", 401)

    data = []
    if field == 'node':
        args = {"network": "plenario_development"}
        fields = ('network',)
        validated_args = sensor_network_validate(IFTTTValidator(only=fields), args)
        data = [{"label": node.id,
                 "value": node.id} for node in get_raw_metadata('nodes', validated_args)]
    elif field == 'curated_property':
        data = [{"label": curated_property,
                 "value": curated_property} for curated_property in curated_map.keys()]

    return make_ifttt_response(data)


def format_ifttt_observations(obs, curated_property):
    """Response format for network metadata.

        :param obs: (Row) from feature table
        :param curated_property: (string) the property drop-down list value,
                                 returned for use in user alert messages
        :returns: (dict) formatted result"""

    obs_response = {
        "node": obs.node_id,
        "datetime": obs.datetime.isoformat() + '+05:00',
        "curated_property": curated_property,
        "value": getattr(obs, curated_map[curated_property].split('.')[1]),
        "meta": {
            "id": uuid.uuid1().hex,
            "timestamp": int(time.time())
        }
    }

    return obs_response


def run_ifttt_queries(queries, curated_property):
    """Run a list of queries, collect results, and return formatted JSON.

       :param queries: (list) of SQLAlchemy query objects
       :param curated_property: (string) the property drop-down list value,
                                returned for use in user alert messages
       :returns: (Response) containing rows fornatted into JSON"""

    data = list()
    for query, table in queries:
        data += [format_ifttt_observations(obs, curated_property) for obs in query.all()]

    data.sort(key=lambda x: parse(x["datetime"]), reverse=True)

    return make_ifttt_response(data)


def make_ifttt_response(data):
    """Format data into response format for IFTTT.

       :param data: (list) list of formatted observation dicts
       :returns: (Response) containing rows fornatted into JSON"""

    resp = {
        "data": data
    }
    resp = make_response(json.dumps(resp, default=unknown_object_json_handler), 200)
    resp.headers['Content-Type'] = 'application/json; charset=utf-8'
    return resp


def make_ifttt_error(err, status_code):
    """Format error response for IFTTT.

       :param err: (string) error message
       :param status_code: (int) status code required by IFTTT
       :returns: (Response) containing errors"""

    resp = {
        "errors": [{"message": err}]
    }
    resp = make_response(json.dumps(resp, default=unknown_object_json_handler), status_code)
    resp.headers['Content-Type'] = 'application/json; charset=utf-8'
    return resp


# ========================
# IFTTT testing endpoints
# ========================


@crossdomain(origin="*")
def ifttt_status():
    """Basic status reponse return for IFTTT endpoint testing.

       :endpoint: ifttt/v1/status
       :returns: (Response) containing errors or nothing"""
    if request.headers.get('IFTTT-Channel-Key') != environ.get('IFTTT_CHANNEL_KEY'):
        return make_ifttt_error("incorrect channel key", 401)

    resp = make_response('{}', 200)
    resp.headers['Content-Type'] = 'application/json'
    return resp


@crossdomain(origin="*")
def ifttt_test_setup():
    """Returns testing data for IFTTT endpoint testing.

       :endpoint: ifttt/v1/test/setup
       :returns: (Response) containing errors or IFTTT-formatted test data dict"""

    if request.headers.get('IFTTT-Channel-Key') != environ.get('IFTTT_CHANNEL_KEY'):
        return make_ifttt_error("incorrect channel key", 401)

    resp = {
        "data": {
            "samples": {
                "triggers": {
                    "property_comparison": {
                        "node": "node_dev_1",
                        "curated_property": "temperature",
                        "op": "gt",
                        "val": 0
                    }
                },
                "triggerFieldValidations": {
                    "property_comparison": {
                        "node": {
                            "valid": "node_dev_1",
                            "invalid": "invalid_node"
                        },
                        "curated_property": {
                            "valid": "temperature",
                            "invalid": "invalid_property"
                        }
                    }
                }
            }
        }
    }

    resp = make_response(json.dumps(resp, default=unknown_object_json_handler), 200)
    resp.headers['Content-Type'] = 'application/json; charset=utf-8'
    return resp
