"""jobs: api endpoint for submitting, monitoring, and deleting jobs"""

import boto.sqs, redis, json
from boto.sqs.message import Message
from os import urandom
import datetime, time

from plenario.settings import CACHE_CONFIG, AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION_NAME, JOBS_QUEUE
from plenario.api.common import unknown_object_json_handler
from plenario.database import session as Session
from flask import request, make_response
from functools32 import wraps

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
    JobsDB.set(CACHE_CONFIG["CACHE_KEY_PREFIX"] + "_job_status_" + ticket, json.dumps(status))


def get_request(ticket):
    return json.loads(JobsDB.get(CACHE_CONFIG["CACHE_KEY_PREFIX"] + "_job_query_" + ticket))


def set_request(ticket, request):
    JobsDB.set(CACHE_CONFIG["CACHE_KEY_PREFIX"] + "_job_query_" + ticket, json.dumps(request))


def get_result(ticket):
    return json.loads(JobsDB.get(CACHE_CONFIG["CACHE_KEY_PREFIX"] + "_job_result_" + ticket))


def set_result(ticket, result):
    JobsDB.set(CACHE_CONFIG["CACHE_KEY_PREFIX"] + "_job_result_" + ticket, json.dumps(result))


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
        resp = {"ticket": ticket, "error": "Job not found."}
        resp = make_response(json.dumps(resp, default=unknown_object_json_handler), 404)
        resp.headers['Content-Type'] = 'application/json'

        return resp

    req = get_request(ticket)
    result = get_result(ticket)
    status = get_status(ticket)


    resp = {"ticket": ticket, "request": req, "result": result, "status": status}
    resp = make_response(json.dumps(resp, default=unknown_object_json_handler), 200)
    resp.headers['Content-Type'] = 'application/json'
    return resp


def post_job():
    # TODO: Establish a friendly form!
    return make_job_response()

#def delete_job(job_id):
#    return '200 Ok: job', job_id, 'removed'


def make_job_response():

    req = {"endpoint": request.path, "query": request.args.to_dict()}

    ticket = submit_job(req)

    resp = {"ticket": ticket, "request": req, "url": request.url_root+"v1/api/jobs/" + ticket}
    resp = make_response(json.dumps(resp, default=unknown_object_json_handler), 200)
    resp.headers['Content-Type'] = 'application/json'
    return resp

# ===========
# Job Methods
# ===========
# jobable: decorator which is responsible for providing the option to submit jobs
# submit_job_record: creates a record to keep track of a job's status and result
# enqueue_message: creates a job message and adds it to a queue for the worker


def jobable(fn):
    """
    Decorating for existing route functions. Allows user to specify if they
    would like to add their query to the job queue and recieve a ticket.

    :param fn: flask route function

    :returns: decorated endpoint
    """

    @wraps(fn)
    def wrapper(validator_result):
        is_job = validator_result.data.get('job')
        if is_job:
            return make_job_response()
        else:
            return fn(validator_result)
    return wrapper


def submit_job(req):
    """
    Submit job to a Plenario Worker via the Amazon SQS Queue and Redis. Return a ticket for the job.

    :param request: The job request

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
        }
    }

    set_status(ticket, {"queued": {"queueTime": str(datetime.datetime.now())}})
    touch_ticket_expiry(ticket)

    #Send this *last* after everything is ready.
    JobsQueue.write(message)

    file = open("/opt/python/log/sender.log", "a")
    file.write("{}: Sent job with ticket {}...\n".format(datetime.datetime.now(), ticket))
    file.close()

    return ticket