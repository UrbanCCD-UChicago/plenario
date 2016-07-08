### THIS IS AN INDEPENDENT PYTHON SCRIPT THAT RUNS THE WORKER ###
### AVOID IMPORTING FROM THIS FILE                            ###

import datetime, time, signal
import boto.sqs
import json
import logging
from os import urandom
from plenario.settings import AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION_NAME, JOBS_QUEUE
from plenario.api.point import _timeseries, _detail, _detail_aggregate, _meta,_grid
from plenario.api.validator import validate, Validator
from plenario.api.response import internal_error, bad_request, json_response_base, make_csv
from plenario.api.jobs import get_status, set_status, get_request, set_result
from flask import Flask

wait_interval = 10
do_work = True
worker_id = urandom(4).encode('hex')

# For development, so we can get loud logs.
logging.basicConfig(level=logging.DEBUG)


def log(msg):
    logging.debug(msg)
    # The constant opening and closing is meh, I know. But I'm feeling lazy
    # right now.
    logfile = open('/opt/python/log/worker.log', "a")
    logfile.write(msg + '\n')
    logfile.close()


# Holds the methods which perform the work requested by an incoming job.
endpoint_logic = {
    'timeseries': lambda args: _timeseries(args),
    'detail-aggregate': lambda args: _detail_aggregate(args),
    'detail': lambda args: _detail(args),
    'meta': lambda args: _meta(args),
    'fields': lambda args: _meta(args),
    'grid': lambda args: _grid(args),
    'ping': lambda args: {'hallo': 'from worker {}'.format(worker_id)}
}


def report_job(tick, status_str):
    return \
        {status_str:
            {
                "workerID": worker_id,
                "queueTime": get_status(tick)["processing"]["queueTime"],
                "startTime": get_status(tick)["processing"]["startTime"],
                "endTime": str(datetime.datetime.now())
            }}


def quit_job(signum, frame):
    global do_work
    log("{} - Worker #{}: Got termination signal. Finishing job...\n"
        .format(datetime.datetime.now(), worker_id))
    do_work = False


signal.signal(signal.SIGINT, quit_job)
signal.signal(signal.SIGTERM, quit_job)

conn = boto.sqs.connect_to_region(
    AWS_REGION_NAME,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)
JobQueue = conn.get_queue(JOBS_QUEUE)

log("{} - Worker #{}: Hello! I'm ready for anything.\n"
    .format(datetime.datetime.now(), worker_id))

while do_work:
    response = JobQueue.get_messages(message_attributes=["ticket"])
    if len(response) > 0:
        job = response[0]
        body = job.get_body()
        if not body == "plenario_job":
            log("{} - Worker #{}: Message is not a Plenario Job. Skipping.\n"
                .format(datetime.datetime.now(), worker_id))
            continue

        try:
            ticket = str(job.message_attributes["ticket"]["string_value"])
        except KeyError:
            log("{} - Worker #{}: Job does not contain a ticket! Removing.\n"
                .format(datetime.datetime.now(), worker_id))
            JobQueue.delete_message(job)

            continue

        log("{} - Worker #{}: Received job with ticket {}.\n"
            .format(datetime.datetime.now(), worker_id, ticket))

        try:
            status = get_status(ticket)
        except Exception as e:
            log("{} - Worker #{}: Job is malformed. Removing.\n"
                .format(datetime.datetime.now(), worker_id))
            JobQueue.delete_message(job)
            print e

            continue
        if "queued" not in status.keys():
            log("{} - Worker #{}: Job has already been started. Skipping.\n"
                .format(datetime.datetime.now(), worker_id))
            continue

        set_status(ticket, report_job(ticket, 'processing'))
        req = get_request(ticket)

        log("{} - Worker #{}: Starting work on ticket {}.\n"
            .format(datetime.datetime.now(), worker_id, ticket))

        ### Do work on query ###
        app = Flask(__name__)
        with app.app_context():

            # Just pluck the endpoint name off the end of /v1/api/endpoint.
            endpoint = req['endpoint'].split('/')[2]
            query_args = req['query']

            logging.debug("WORKER ENDPOINT: {}".format(endpoint))
            logging.debug("WORKER REQUEST ARGS: {}".format(query_args))

            if endpoint in endpoint_logic:
                set_result(ticket, endpoint_logic[endpoint](query_args))
                set_status(ticket, report_job(ticket, 'success'))

        JobQueue.delete_message(job)

        log("{} - Worker #{}: Finished work on ticket {}.\n"
            .format(datetime.datetime.now(), worker_id, ticket))

    else:
        #file.write("{} - Worker #{}: No work. Idling for {} seconds.\n".format(datetime.datetime.now(), worker_id,
        #                                                                      wait_interval))

        # No work! Idle for a bit to save compute cycles.
        time.sleep(wait_interval)

log("{} - Worker #{}: Exited run loop. Goodbye!\n"
    .format(datetime.datetime.now(), worker_id, wait_interval))
