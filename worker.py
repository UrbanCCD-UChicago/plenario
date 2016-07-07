### THIS IS AN INDEPENDENT PYTHON SCRIPT THAT RUNS THE WORKER ###
### AVOID IMPORTING FROM THIS FILE                            ###

import datetime, time, signal
import boto.sqs
import json
from os import urandom
from plenario.settings import AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION_NAME, JOBS_QUEUE
from plenario.api.point import _timeseries
from plenario.api.validator import NoDefaultDatesValidator, validate, NoGeoJSONValidator, has_tree_filters
from plenario.api.response import internal_error, bad_request, json_response_base, make_csv
from plenario.api.jobs import get_status, set_status, get_request, set_result
from flask import Flask

wait_interval = 10
do_work = True
worker_id = urandom(4).encode('hex')


def quit_job(signum, frame):
    global do_work
    file = open("/opt/python/log/worker.log", "a")
    file.write("{} - Worker #{}: Got termination signal. Finishing job...\n".format(datetime.datetime.now(), worker_id))
    file.close()
    do_work = False


signal.signal(signal.SIGINT, quit_job)
signal.signal(signal.SIGTERM, quit_job)

conn = boto.sqs.connect_to_region(
    AWS_REGION_NAME,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)
JobQueue = conn.get_queue(JOBS_QUEUE)

file = open("/opt/python/log/worker.log", "a")
file.write("{} - Worker #{}: Hello! I'm ready for anything.\n".format(datetime.datetime.now(), worker_id))

while do_work:
    response = JobQueue.get_messages(message_attributes=["ticket"])
    file = open("/opt/python/log/worker.log", "a")
    if len(response) > 0:
        job = response[0]
        body = job.get_body()
        if not body == "plenario_job":
            file.write("{} - Worker #{}: Message is not a Plenario Job. Skipping.\n".format(datetime.datetime.now(),
                                                                                            worker_id))
            file.close()
            continue

        try:
            ticket = str(job.message_attributes["ticket"]["string_value"])
        except:
            file.write("{} - Worker #{}: Job does not contain a ticket! Removing.\n".format(datetime.datetime.now(),
                                                                                            worker_id))
            file.close()

            JobQueue.delete_message(job)

            continue

        file.write("{} - Worker #{}: Received job with ticket {}.\n".format(datetime.datetime.now(),
                                                                            worker_id, ticket))

        try:
            status = get_status(ticket)
        except Exception as e:
            file.write("{} - Worker #{}: Job is malformed. Removing.\n".format(datetime.datetime.now(),
                                                                               worker_id))
            file.close()
            JobQueue.delete_message(job)
            print e

            continue
        if not "queued" in status.keys():
            file.write("{} - Worker #{}: Job has already been started. Skipping.\n".format(datetime.datetime.now(),
                                                                                           worker_id))
            file.close()
            continue

        set_status(ticket, {"processing": {"workerID": worker_id, "queueTime": status["queued"]["queueTime"],
                                           "startTime": str(datetime.datetime.now())}})

        req = get_request(ticket)

        file.write("{} - Worker #{}: Starting work on ticket {}.\n".format(datetime.datetime.now(), worker_id, ticket))
        file.close()

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

        file = open("/opt/python/log/worker.log", "a")
        file.write("{} - Worker #{}: Finished work on ticket {}.\n".format(datetime.datetime.now(), worker_id, ticket))

    else:
        #file.write("{} - Worker #{}: No work. Idling for {} seconds.\n".format(datetime.datetime.now(), worker_id,
        #                                                                      wait_interval))

        # No work! Idle for a bit to save compute cycles.
        time.sleep(wait_interval)

    file.close()

file = open("/opt/python/log/worker.log", "a")
file.write("{} - Worker #{}: Exited run loop. Goodbye!\n".format(datetime.datetime.now(), worker_id, wait_interval))
file.close()
