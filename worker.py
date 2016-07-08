###############################################################
# THIS IS AN INDEPENDENT PYTHON SCRIPT THAT RUNS THE WORKER   #
# DO NOT IMPORT THIS FILE                                     #
#                                                             #
# `python worker.py` should only be called one time.          #
# This program will set up the necessary Plenario resources   #
# (particularly the Postgres connection pool) which are       #
# shared between worker threads. It then spawns the           #
# appropriate number of worker threads and watches over them. #
###############################################################

import datetime, time, signal
import boto.sqs
import json
import logging
import random
import threading
from os import urandom
from plenario.settings import AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION_NAME, JOBS_QUEUE
from plenario.api.point import _timeseries, _detail, _detail_aggregate, _meta,_grid
from plenario.api.validator import validate, Validator
from plenario.api.response import internal_error, bad_request, json_response_base, make_csv
from plenario.api.jobs import get_status, set_status, get_request, set_result
from flask import Flask

worker_threads = 4
max_wait_interval = 10
do_work = True

# For development, so we can get loud logs.
logging.basicConfig(level=logging.DEBUG)


def log(msg, id):
    logging.debug(msg)
    # The constant opening and closing is meh, I know. But I'm feeling lazy
    # right now.
    logfile = open('/opt/python/log/worker.log', "a")
    logfile.write("{} - Worker #{}: {}\n".format(datetime.datetime.now(), id, msg) + '\n')
    logfile.close()


# Holds the methods which perform the work requested by an incoming job.
endpoint_logic = {
    'timeseries': lambda args: _timeseries(args),
    'detail-aggregate': lambda args: _detail_aggregate(args),
    'detail': lambda args: _detail(args),
    'meta': lambda args: _meta(args),
    'fields': lambda args: _meta(args),
    'grid': lambda args: _grid(args),
}


def report_job(tick, status_str, id):
    return \
        {status_str:
            {
                "workerID": id,
                "queueTime": get_status(tick)["processing"]["queueTime"],
                "startTime": get_status(tick)["processing"]["startTime"],
                "endTime": str(datetime.datetime.now())
            }}


def stop_workers(signum, frame):
    global do_work
    log("Got termination signal. Finishing job...", "WORKER BOSS")
    do_work = False

signal.signal(signal.SIGINT, stop_workers)
signal.signal(signal.SIGTERM, stop_workers)

conn = boto.sqs.connect_to_region(
    AWS_REGION_NAME,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)
JobQueue = conn.get_queue(JOBS_QUEUE)


def worker():
    worker_id = urandom(4).encode('hex')

    endpoint_logic['ping'] = lambda args: {'hello': 'from worker id {}'.format(worker_id)}

    log("Hello! I'm ready for anything.", worker_id)

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

            set_status(ticket, report_job(ticket, 'processing', worker_id))
            req = get_request(ticket)

            log("{} - Worker #{}: Starting work on ticket {}.\n"
                .format(datetime.datetime.now(), worker_id, ticket))

            ### Do work on query ###
            app = Flask(__name__)
            with app.app_context():

                # Just pluck the endpoint name off the end of /v1/api/endpoint.
                endpoint = req['endpoint'].split('/')[-1]
                query_args = req['query']

                logging.debug("WORKER ENDPOINT: {}".format(endpoint))
                logging.debug("WORKER REQUEST ARGS: {}".format(query_args))

                if endpoint in endpoint_logic:
                    set_result(ticket, endpoint_logic[endpoint](query_args))
                    set_status(ticket, report_job(ticket, 'success', worker_id))


            log("Finished work on ticket {}.".format(ticket), worker_id);

        else:
            # No work! Idle for a bit to save compute cycles.
            # This interval is random in order to stagger workers
            idle = random.randrange(max_wait_interval)
            log("Ho hum nothing to do. Idling for {} seconds.".format(idle))
            time.sleep(idle)

    log("Exited run loop. Goodbye!", worker_id)

threads = []
for i in range(worker_threads):
    t = threading.Thread(target=worker, name="plenario-worker-thread")
    t.daemon = False
    threads.append(t)
    t.start()

for t in threads:
    while True:
        t.join(60)
        if not t.is_alive():
            break

log("All workers have exited.", "WORKER BOSS")