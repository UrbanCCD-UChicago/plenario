"""jobs: api endpoint for submitting, monitoring, and deleting jobs"""

import datetime
import json
import redis
import time
import pickle
import warnings


from binascii import hexlify
from flask import request, jsonify
from os import urandom

from plenario.database import Base, app_engine as engine
from plenario.settings import CACHE_CONFIG
from plenario.api.common import unknown_object_json_handler
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

    celery_taskmeta = reflect("celery_taskmeta", Base.metadata, engine)
    query = celery_taskmeta.select().where(celery_taskmeta.c.task_id == ticket)
    job_meta = dict(query.execute().first().items())
    job_meta["result"] = pickle.loads(job_meta["result"])

    return job_meta


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

