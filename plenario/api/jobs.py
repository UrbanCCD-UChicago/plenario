"""jobs: api endpoint for submitting, monitoring, and deleting jobs"""

import boto.sqs
import redis
import json
import datetime
import time

from plenario.settings import CACHE_CONFIG, AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION_NAME, JOBS_QUEUE
from plenario.api.common import unknown_object_json_handler
from flask import request, make_response
from boto.sqs.message import Message
from os import urandom

# ========================================= #
# HTTP      URI         ACTION              #
# ========================================= #
# GET       /jobs/:id   retrieve job status #
# POST      /jobs       submit new job form #
# DELETE    /jobs/:id   cancel ongoing job  #
# ========================================= #

redisPool = redis.ConnectionPool(host=CACHE_CONFIG["CACHE_REDIS_HOST"], port=6379, db=0)
JobsDB = redis.Redis(connection_pool=redisPool)


def get_status(ticket):
    return json.loads(JobsDB.get(CACHE_CONFIG["CACHE_KEY_PREFIX"] + "_job_status_" + ticket))


def set_status(ticket, status):
    JobsDB.set(CACHE_CONFIG["CACHE_KEY_PREFIX"] + "_job_status_" + ticket, json.dumps(status, default=unknown_object_json_handler))


def get_request(ticket):
    return json.loads(JobsDB.get(CACHE_CONFIG["CACHE_KEY_PREFIX"] + "_job_query_" + ticket))


def set_request(ticket, request):
    JobsDB.set(CACHE_CONFIG["CACHE_KEY_PREFIX"] + "_job_query_" + ticket, json.dumps(request, default=unknown_object_json_handler))


def get_result(ticket):
    return json.loads(JobsDB.get(CACHE_CONFIG["CACHE_KEY_PREFIX"] + "_job_result_" + ticket))


def set_result(ticket, result):
    JobsDB.set(CACHE_CONFIG["CACHE_KEY_PREFIX"] + "_job_result_" + ticket, json.dumps(result, default=unknown_object_json_handler))


def get_flag(flag):
    return JobsDB.get(CACHE_CONFIG["CACHE_KEY_PREFIX"] + "_job_flag_" + flag)=="true"


def set_flag(flag, state, expire=60):
    JobsDB.set(CACHE_CONFIG["CACHE_KEY_PREFIX"] + "_job_flag_" + flag, "true" if state else "false")
    JobsDB.expire(CACHE_CONFIG["CACHE_KEY_PREFIX"] + "_job_flag_" + flag, expire)


def touch_ticket_expiry(ticket):
    # Expire all job handles in 1 hour.
    JobsDB.expire(CACHE_CONFIG["CACHE_KEY_PREFIX"] + "_job_query_" + ticket, 3600)
    JobsDB.expire(CACHE_CONFIG["CACHE_KEY_PREFIX"] + "_job_result_" + ticket, 3600)
    JobsDB.expire(CACHE_CONFIG["CACHE_KEY_PREFIX"] + "_job_status_" + ticket, 3600)


def ticket_exists(ticket):
    return not JobsDB.get(CACHE_CONFIG["CACHE_KEY_PREFIX"]+"_job_query_"+ticket) == None


def worker_ready():
    try:
        job = {"endpoint": "ping", "query":{}}
        ticket = submit_job(job)
        time.sleep(1)   # Wait for worker to get started. Modify as necessary.
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

    response = {"ticket": ticket, "request": req, "result": result, "status": status}
    response = make_response(json.dumps(response, default=unknown_object_json_handler), 200)
    response.headers['Content-Type'] = 'application/json'
    return response


# def post_job():
#     # TODO: Establish a friendly form!
#     return make_job_response()

#def delete_job(job_id):
#    return '200 Ok: job', job_id, 'removed'


def make_job_response(endpoint, validated_query):

    req = {"endpoint": endpoint, "query": validated_query.data}

    ticket = submit_job(req)

    response = {"ticket": ticket, "request": req, "url": request.url_root+"v1/api/jobs/" + ticket}
    response = make_response(json.dumps(response, default=unknown_object_json_handler), 200)
    response.headers['Content-Type'] = 'application/json'
    return response

# ===========
# Job Methods
# ===========
# jobable: decorator which is responsible for providing the option to submit jobs
# submit_job_record: creates a record to keep track of a job's status and result
# enqueue_message: creates a job message and adds it to a queue for the worker


# def jobable(fn):
#     """
#     Decorating for existing route functions. Allows user to specify if they
#     would like to add their query to the job queue and recieve a ticket.
#
#     :param fn: flask route function
#
#     :returns: decorated endpoint
#     """
#     is_job = validator_result.data.get('job')
#     if is_job:
#         return make_job_response()
#     else:
#         return fn(validator_result)


def submit_job(req):
    """
    Submit job to a Plenario Worker via the Amazon SQS Queue and Redis. Return a ticket for the job.

    :param req: The job request

    :returns: ticket: An id that can be used to fetch the job results and status.
    """

    #Quickly generate a random hash. Collisions are highly unlikely!
    #Seems safer than relying on a counter in the database.
    ticket = urandom(16).encode('hex')

    conn = boto.sqs.connect_to_region(
        AWS_REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    JobsQueue = conn.get_queue(JOBS_QUEUE)

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

    #Send this *last* after everything is ready.
    JobsQueue.write(message)

    file = open("/opt/python/log/sender.log", "a")
    file.write("{}: Sent job with ticket {}...\n".format(datetime.datetime.now(), ticket))
    file.close()

    return ticket


def cancel_job(ticket):
    conn = boto.sqs.connect_to_region(
        AWS_REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    JobsQueue = conn.get_queue(JOBS_QUEUE)

    response = JobsQueue.get_messages(message_attributes=[ticket])
    if len(response) > 0:
        JobsQueue.delete_message(response[0])
        return ticket
    return None