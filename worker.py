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
import random
import threading
from os import urandom
from plenario.settings import AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION_NAME, JOBS_QUEUE
from plenario.api.point import _timeseries
from plenario.api.validator import NoDefaultDatesValidator, validate, NoGeoJSONValidator, has_tree_filters
from plenario.api.response import internal_error, bad_request, json_response_base, make_csv
from plenario.api.jobs import get_status, set_status, get_request, set_result
from flask import Flask

worker_threads = 4

max_wait_interval = 10
do_work = True

def stop_workers(signum, frame):
    global do_work
    file = open("/opt/python/log/worker.log", "a")
    file.write("{} - Worker BOSS: Got termination signal. Telling workers to finish up...\n".format(datetime.datetime.now()))
    file.close()
    do_work = False

signal.signal(signal.SIGINT, stop_workers)
signal.signal(signal.SIGTERM, stop_workers)

conn = boto.sqs.connect_to_region(
    AWS_REGION_NAME,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)
JobQueue = conn.get_queue(JOBS_QUEUE)

worker_id = "WORKER BOSS"

def worker():
    worker_id = urandom(4).encode('hex')

    def log(message):
        file = open("/opt/python/log/worker.log", "a")
        file.write("{} - Worker #{}: {}\n".format(datetime.datetime.now(), worker_id, message))
        file.close()

    log("Hello! I'm ready for anything.")

    while do_work:
        response = JobQueue.get_messages(message_attributes=["ticket"])
        file = open("/opt/python/log/worker.log", "a")
        if len(response) > 0:
            job = response[0]
            body = job.get_body()
            if not body == "plenario_job":
                log("Message is not a Plenario Job. Skipping.")
                continue

            try:
                ticket = str(job.message_attributes["ticket"]["string_value"])
            except:
                log("Job does not contain a ticket! Removing.")
                JobQueue.delete_message(job)
                continue

            log("Received job with ticket {}.".format(ticket))

            try:
                status = get_status(ticket)
            except Exception as e:
                log("Job is malformed ({}). Removing.".format(e))
                JobQueue.delete_message(job)
                continue

            if not "queued" in status.keys():
                log("Job has already been started. Skipping.")
                continue

            set_status(ticket, {"processing": {"workerID": worker_id, "queueTime": status["queued"]["queueTime"],
                                               "startTime": str(datetime.datetime.now())}})

            req = get_request(ticket)

            log("Starting work on ticket {}.".format(ticket))

            ### Do work on query ###
            app = Flask(__name__)
            with app.app_context():
                # timeseries
                if req["endpoint"] in ["/v1/api/timeseries", "/v1/api/timeseries/"]:
                    fields = ('location_geom__within', 'dataset_name', 'dataset_name__in',
                              'agg', 'obs_date__ge', 'obs_date__le', 'data_type')

                    validator = NoGeoJSONValidator(only=fields)
                    validated_args = validate(validator, req["query"])
                    if validated_args.errors:
                        set_result(ticket, bad_request(validated_args.errors))
                        set_status(ticket, {
                            "error": {"workerID": worker_id, "queueTime": get_status(ticket)["processing"]["queueTime"],
                                      "startTime": get_status(ticket)["processing"]["startTime"], "endTime": str(datetime.datetime.now())}})
                            #TODO: Add more detailed error messages here

                    set_result(ticket, json.loads(_timeseries(validated_args).get_data(as_text=True)))
                    set_status(ticket, {
                        "success": {"workerID": worker_id, "queueTime": get_status(ticket)["processing"]["queueTime"],
                                    "startTime": get_status(ticket)["processing"]["startTime"], "endTime": str(datetime.datetime.now())}})
                elif req["endpoint"] in ["ping"]:
                    set_result(ticket, {"hello": "from worker {}".format(worker_id)})
                    set_status(ticket, {
                        "success": {"workerID": worker_id, "queueTime": get_status(ticket)["processing"]["queueTime"],
                                    "startTime": get_status(ticket)["processing"]["startTime"], "endTime": str(datetime.datetime.now())}})

            JobQueue.delete_message(job)

            log("Finished work on ticket {}.".format(ticket));

        else:
            # No work! Idle for a bit to save compute cycles.
            #This interval is random in order to stagger workers
            idle = random.randrange(max_wait_interval)
            log("Ho hum nothing to do. Idling for {} seconds.".format(idle))
            time.sleep(idle)

    log("Exited run loop. Goodbye!")

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

file = open("/opt/python/log/worker.log", "a")
file.write("{} - Worker BOSS: All workers have exited.\n".format(datetime.datetime.now()))
file.close()
