"""jobs: api endpoint for submitting, monitoring, and deleting jobs"""

import datetime
import json
import redis
import response as api_response
import time

from boto.sqs.message import Message
from flask import request, make_response
from os import urandom

from plenario.settings import CACHE_CONFIG, JOBS_QUEUE
from plenario.api.common import unknown_object_json_handler
from plenario.utils.model_helpers import fetch_table
from plenario_worker.clients import sqs_client

redisPool = redis.ConnectionPool(host=CACHE_CONFIG["CACHE_REDIS_HOST"], port=6379, db=0)
JobsDB = redis.Redis(connection_pool=redisPool)
JobsQueue = sqs_client.get_queue(JOBS_QUEUE)


def get_status(ticket):
    return json.loads(JobsDB.get(CACHE_CONFIG["CACHE_KEY_PREFIX"] + "_job_status_" + ticket))


def set_status(ticket, status):
    JobsDB.set(CACHE_CONFIG["CACHE_KEY_PREFIX"] + "_job_status_" + ticket,
               json.dumps(status, default=unknown_object_json_handler))


def get_request(ticket):
    return json.loads(JobsDB.get(CACHE_CONFIG["CACHE_KEY_PREFIX"] + "_job_query_" + ticket))


def set_request(ticket, request_):
    JobsDB.set(CACHE_CONFIG["CACHE_KEY_PREFIX"] + "_job_query_" + ticket,
               json.dumps(request_, default=unknown_object_json_handler))


def get_result(ticket):
    return json.loads(JobsDB.get(CACHE_CONFIG["CACHE_KEY_PREFIX"] + "_job_result_" + ticket))


def set_result(ticket, result):
    JobsDB.set(CACHE_CONFIG["CACHE_KEY_PREFIX"] + "_job_result_" + ticket,
               json.dumps(result, default=unknown_object_json_handler))


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
        ticket = submit_job(job)
        time.sleep(1)  # Wait for worker to get started. Modify as necessary.
        return "hello" in get_result(ticket).keys()
    except:
        return False


def get_job(ticket):
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
            return api_response.export_dataset_to_response(shapeset, data_type, result)
        elif hasattr(req['query'], 'get') and req['query'].get('data_type') == 'csv':
            # Exports CSV files for aggregate-point-data and detail-aggregate.
            # This method appends geom to remove on its own.
            return api_response.form_csv_detail_response([], result)

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
    ticket = urandom(16).encode('hex')

    set_request(ticket, req)
    set_result(ticket, "")

    message = Message()
    message.set_body("plenario_job")
    message.message_attributes = {
        "ticket": {
            "data_type": "String",
            "string_value": str(ticket)
        },
        # For getting job by ticket
        str(ticket): {
            "data_type": "String",
            "string_value": "ticket"
        }
    }

    set_status(ticket, {"status": "queued", "meta": {"queueTime": str(datetime.datetime.now())}})
    touch_ticket_expiry(ticket)

    # Send this *last* after everything is ready.
    JobsQueue.write(message)

    file_ = open("/opt/python/log/sender.log", "a")
    file_.write("{}: Sent job with ticket {}...\n".format(datetime.datetime.now(), ticket))
    file_.close()

    return ticket


def cancel_job(ticket):

    response = JobsQueue.get_messages(message_attributes=[ticket])
    if len(response) > 0:
        JobsQueue.delete_message(response[0])
        return ticket
    return None
