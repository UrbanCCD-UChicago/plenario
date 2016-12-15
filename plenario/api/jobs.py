"""jobs: api endpoint for submitting, monitoring, and deleting jobs"""

import datetime
import json
import redis
import time
import warnings


from binascii import hexlify
from flask import request, make_response
from os import urandom

from plenario.database import Base, app_engine as engine
from plenario.settings import CACHE_CONFIG
from plenario.api.common import unknown_object_json_handler
from plenario.api.response import export_dataset_to_response
from plenario.api.response import form_csv_detail_response
from plenario.utils.helpers import reflect
from plenario.utils.model_helpers import fetch_table
# from plenario_worker.clients import job_queue

redisPool = redis.ConnectionPool(host=CACHE_CONFIG["CACHE_REDIS_HOST"], port=6379, db=0)
JobsDB = redis.Redis(connection_pool=redisPool)

prefix = CACHE_CONFIG["CACHE_KEY_PREFIX"]


def get_status(ticket: str):
    cache_key = prefix + "_job_status_" + ticket
    try:
        return json.loads(JobsDB.get(cache_key).decode("utf-8"))
    except AttributeError:
        return None


def set_status(ticket: str, status):
    cache_key = prefix + "_job_status_" + ticket
    JobsDB.set(
        name=cache_key,
        value=json.dumps(status, default=unknown_object_json_handler)
    )


def get_request(ticket: str):
    cache_key = prefix + "_job_query_" + ticket
    return json.loads(JobsDB.get(cache_key).decode("utf-8"))


def set_request(ticket: str, request_):
    cache_key = prefix + "_job_query_" + ticket
    JobsDB.set(
        name=cache_key,
        value=json.dumps(request_, default=unknown_object_json_handler)
    )


def get_result(ticket: str):
    cache_key = prefix + "_job_result_" + ticket
    return json.loads(JobsDB.get(cache_key).decode("utf-8"))


def set_result(ticket: str, result):
    cache_key = prefix + "_job_result_" + ticket
    JobsDB.set(
        name=cache_key,
        value=json.dumps(result, default=unknown_object_json_handler)
    )


def get_flag(flag):
    return JobsDB.get(CACHE_CONFIG["CACHE_KEY_PREFIX"] + "_job_flag_" + flag) == "true"


def set_flag(flag, state, expire=60):
    JobsDB.set(CACHE_CONFIG["CACHE_KEY_PREFIX"] + "_job_flag_" + flag, "true" if state else "false")
    JobsDB.expire(CACHE_CONFIG["CACHE_KEY_PREFIX"] + "_job_flag_" + flag, expire)


def touch_ticket_expiry(ticket):
    # Expire all job handles in 3 hours.
    JobsDB.expire(CACHE_CONFIG["CACHE_KEY_PREFIX"] + "_job_query_" + ticket, 10800)
    JobsDB.expire(CACHE_CONFIG["CACHE_KEY_PREFIX"] + "_job_result_" + ticket, 10800)
    JobsDB.expire(CACHE_CONFIG["CACHE_KEY_PREFIX"] + "_job_status_" + ticket, 10800)


def ticket_exists(ticket):
    return not JobsDB.get(CACHE_CONFIG["CACHE_KEY_PREFIX"] + "_job_query_" + ticket) is None


def worker_ready():
    try:
        job = {"endpoint": "ping", "query": {}}
        submit_job(job)
        ticket = submit_job(job)
        time.sleep(1)  # Wait for worker to get started. Modify as necessary.
        return "hello" in list(get_result(ticket).keys())
    except:
        return False


def get_job(ticket: str):
    if not ticket_exists(ticket):
        response = {"ticket": ticket, "error": "Job not found."}
        response = make_response(json.dumps(response, default=unknown_object_json_handler), 404)
        response.headers['Content-Type'] = 'application/json'
        return response

    req = get_request(ticket)
    result = get_result(ticket)
    status = get_status(ticket)

    if status['status'] == "success":
        # Here we need to do some legwork to find out what kind of response the
        # user wants. This is to address endpoints such as export-shape, which
        # can return many different downloadable format types.
        if req['endpoint'] == "export-shape":
            # TODO: Here, the work is not being done by the worker. Need to
            # TODO: correct this.
            shapeset = fetch_table(req['query']['shapeset'])
            data_type = req['query']['data_type']
            return export_dataset_to_response(shapeset, data_type, result)
        elif hasattr(req['query'], 'get') and req['query'].get('data_type') == 'csv':
            # Exports CSV files for aggregate-point-data and detail-aggregate.
            # This method appends geom to remove on its own.
            return form_csv_detail_response([], result)

    response = {"ticket": ticket, "request": req, "result": result, "status": status}
    response = make_response(json.dumps(response, default=unknown_object_json_handler), 200)
    response.headers['Content-Type'] = 'application/json'

    return response


def make_job_response(endpoint, validated_query):
    req = {"endpoint": endpoint, "query": validated_query.data}

    ticket = submit_job(req)

    response = {"ticket": ticket, "request": req, "url": request.url_root + "v1/api/jobs/" + ticket}
    response = make_response(json.dumps(response, default=unknown_object_json_handler), 200)
    response.headers['Content-Type'] = 'application/json'
    return response


def submit_job(req):
    """
    Submit job to a Plenario Worker via the Amazon SQS Queue and Redis. Return a ticket for the job.

    :param req: The job request

    :returns: ticket: An id that can be used to fetch the job results and status.
    """

    # Quickly generate a random hash. Collisions are highly unlikely!
    # Seems safer than relying on a counter in the database.

    ticket = hexlify(urandom(16)).decode("utf-8")

    set_request(ticket, req)
    set_result(ticket, "")

    set_status(ticket, {"status": "queued", "meta": {"queueTime": str(datetime.datetime.now())}})
    touch_ticket_expiry(ticket)

    # # Send this *last* after everything is ready.
    # if job_queue is not None:
    #     job_queue.send_message(
    #         MessageBody="plenario_job",
    #         MessageAttributes={
    #             "ticket": {
    #                 "DataType": "String",
    #                 "StringValue": str(ticket)
    #             }, str(ticket): {
    #                 "DataType": "String",
    #                 "StringValue": "ticket"
    #             }
    #         }
    #     )
    # else:
    #     temporary_worker(ticket, req)

    try:
        logfile = open("/opt/python/log/sender.log", "a")
    except IOError:
        warnings.warn("Failed to write to /opt/python/log/sender.log - "
                      "writing to current directory.", RuntimeWarning)
        logfile = open("./sender.log", "a")
    logfile.write("{}: Sent job with ticket {}...\n".format(datetime.datetime.now(), ticket))
    logfile.close()

    return ticket


def cancel_job(ticket):

    # response = job_queue.receive_messages(message_attributes=[ticket])
    # if len(response) > 0:
        # job_queue.delete_message(response[0])
    return ticket
    # return None


# def temporary_worker(ticket, job_request):
#     """For situations in local development where queue system is not present,
#     spin up a temporary worker thread to accomplish a job right away. This is
#     a very distilled version of worker.py - possibly a good reference for
#     learning about the main worker system.
#
#     :param ticket: (str) ID of job status stored within Redis
#     :param job_request: (dict) contains API endpoint and query arguments"""
#
#     import traceback
#
#     from collections import namedtuple
#     from threading import Thread
#
#     from plenario.api.response import convert_result_geoms
#     from plenario.api.validator import convert
#     from plenario_worker.utilities import log
#     from plenario_worker.endpoints import endpoint_logic, shape_logic
#     from plenario_worker.endpoints import etl_logic
#     from plenario_worker.ticket import set_ticket_error, set_ticket_queued
#     from plenario_worker.ticket import set_ticket_success
#
#     ValidatorProxy = namedtuple("ValidatorProxy", ["data"])
#
#     endpoint = job_request["endpoint"]
#     query_args = job_request["query"]
#     status = {"meta": {"startTime": datetime.datetime.now()}}
#     set_ticket_queued(status, ticket, "Queued", "temp")
#
#     # Note that this section is idential to logic check in worker.py
#     # TODO: Extract this and the worker's into a separate method
#     def do_work(endpoint, query_args, ticket):
#         try:
#             if endpoint in endpoint_logic:
#                 # These keys are used for the datadump endpoint
#                 query_args["jobsframework_ticket"] = ticket
#                 query_args["jobsframework_workerid"] = "temp"
#                 query_args["jobsframework_workerbirthtime"] = datetime.datetime.now()
#                 convert(query_args)
#                 query_args = ValidatorProxy(query_args)
#                 result = endpoint_logic[endpoint](query_args)
#             elif endpoint in shape_logic:
#                 convert(query_args)
#                 query_args = ValidatorProxy(query_args)
#                 result = shape_logic[endpoint](query_args)
#                 if endpoint == 'aggregate-point-data' and query_args.data.get('data_type') != 'csv':
#                     result = convert_result_geoms(result)
#             elif endpoint in etl_logic:
#                 if endpoint in ('update_weather', 'update_metar'):
#                     result = etl_logic[endpoint]()
#                 else:
#                     result = etl_logic[endpoint](query_args)
#             else:
#                 set_ticket_error(status, ticket, "Invalid endpoint specified.",  "temp")
#             set_ticket_success(ticket, result)
#         except Exception as exc:
#             set_ticket_error(status, ticket, traceback.format_exc(exc), "temp")
#
#     temp_thread = Thread(target=lambda: do_work(endpoint, query_args, ticket))
#     temp_thread.start()
#     log("Beginning work on endpoint: {}, with args: {}"
#         .format(endpoint, query_args), "temp")
