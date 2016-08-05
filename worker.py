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

import os
import signal
import threading
import time
import traceback

from collections import namedtuple
from datetime import datetime
from flask import Flask
from subprocess import check_output


worker_threads = 8
wait_interval = 1
job_timeout = 3600

if __name__ == "__main__":

    from plenario.api.jobs import get_status, set_status, get_request
    from plenario.api.jobs import set_result, submit_job
    from plenario.api.response import convert_result_geoms
    from plenario.api.validator import convert
    from plenario.database import session
    from plenario.settings import JOBS_QUEUE

    from plenario_worker.clients import autoscaling_client, sqs_client
    from plenario_worker.endpoints import endpoint_logic, etl_logic, shape_logic
    from plenario_worker.names import generate_name
    from plenario_worker.ticket import set_ticket_error
    from plenario_worker.utilities import deregister_job, register_job
    from plenario_worker.utilities import deregister_worker, register_worker
    from plenario_worker.utilities import check_in, log, update_instance_protection

    worker_boss = {
        'active_worker_count': 0,
        'do_work': True,
        'protected': False,
    }

    ValidatorProxy = namedtuple("ValidatorProxy", ["data"])

    JobsQueue = sqs_client.get_queue(JOBS_QUEUE)


    def stop_workers(signum, frame):
        global worker_boss
        log("Got termination signal. Finishing job...", "WORKER BOSS")
        worker_boss['do_work'] = False

    signal.signal(signal.SIGINT, stop_workers)
    signal.signal(signal.SIGTERM, stop_workers)


    def worker():

        app = Flask(__name__)
        worker_id = generate_name()
        birthtime = time.time()

        log("Hello! I'm ready for anything.", worker_id)
        register_worker(birthtime, worker_id)

        with app.app_context():

            throttle = 5
            while worker_boss['do_work']:

                if throttle < 0:
                            check_in(birthtime, worker_id)
                            throttle = 5
                throttle -= 1

                response = JobsQueue.get_messages(message_attributes=["ticket"])
                if len(response) > 0:
                    job = response[0]
                    body = job.get_body()

                    # We need this check to avoid extraneous jobs
                    # submitted by AWS Elastic Beanstalk.
                    if not body == "plenario_job":
                        log("Message is not a Plenario Job. Skipping.", worker_id)
                        continue

                    try:
                        ticket = str(job.message_attributes["ticket"]["string_value"])
                    except KeyError:
                        log("ERROR: Job does not contain a ticket! Removing.", worker_id)
                        JobsQueue.delete_message(job)
                        continue

                    log("Received job with ticket {}.".format(ticket), worker_id)

                    try:
                        status = get_status(ticket)
                        assert status["status"] is not None
                        assert status["meta"] is not None
                    except Exception as e:
                                      log("ERROR: Job is malformed ({}). Removing.".format(e), worker_id)
                                      JobsQueue.delete_message(job)
                                      continue

                    # Handle orphaned jobs
                    is_processing = status["status"] == "processing"
                    is_longrunning = "longrunning" in get_request(ticket)
                    is_deferred = status["meta"].get("lastStartTime") is not None
                    # TODO: Make pretty.
                    is_expired = (datetime.now() - datetime.strptime(status["meta"]["startTime"], "%Y-%m-%d %H:%M:%S.%f")).total_seconds() > job_timeout
                    deferral_expired = (datetime.now() - datetime.strptime(status["meta"]["lastStartTime"], "%Y-%m-%d %H:%M:%S.%f")).total_seconds() > job_timeout
                    if (is_processing and
                        not is_longrunning and
                            ((not is_deferred and is_expired) or (is_deferred and deferral_expired))):

                        status["meta"]["tries"] = status["meta"]["tries"] + 1 \
                            if status["meta"].get("tries") else 1

                        # Only try orphaned jobs again once
                        if status["meta"]["tries"] > 1:
                            error_msg = "Stalled task {}. Removing.".format(ticket)
                            set_ticket_error(status, ticket, error_msg, worker_id)

                            JobsQueue.delete_message(job)
                            deregister_job(birthtime, worker_id)

                            worker_boss['active_worker_count'] -= 1
                            update_instance_protection(worker_boss, autoscaling_client)

                            continue
                        else:
                            log("WARNING: Ticket {} has been orphaned...retrying.".format(ticket), worker_id)
                            status["meta"]["lastDeferredTime"] = str(datetime.now())
                            set_status(ticket, status)
                        traceback.print_exc()

                    # Standard job mutex
                    elif status["status"] != "queued":
                        log("Job has already been started. Skipping.", worker_id)
                        continue

                    status["status"] = "processing"

                    if "lastDeferredTime" in status["meta"]:
                        status["meta"]["lastResumeTime"] = str(datetime.now())
                    else:
                        status["meta"]["startTime"] = str(datetime.now())

                    if "workers" not in status["meta"]:
                        status["meta"]["workers"] = []

                    status["meta"]["workers"].append(worker_id)
                    set_status(ticket, status)

                    # =========== Do work on query =========== #
                    try:
                        log("Starting work on ticket {}.".format(ticket), worker_id)
                        register_job(ticket, birthtime, worker_id)

                        req = get_request(ticket)
                        endpoint = req['endpoint']
                        query_args = req['query']

                        if endpoint in endpoint_logic:

                            # Add worker metadata
                            query_args["jobsframework_ticket"] = ticket
                            query_args["jobsframework_workerid"] = worker_id
                            query_args["jobsframework_workerbirthtime"] = birthtime

                            # Because we're getting serialized arguments from Redis,
                            # we need to convert them back into a validated form.
                            convert(query_args)
                            query_args = ValidatorProxy(query_args)

                            log("Ticket {}: endpoint {}.".format(ticket, endpoint), worker_id)

                            result = endpoint_logic[endpoint](query_args)

                            # Check for metacommands
                            if "jobsframework_metacommands" in result:
                                defer = False
                                stop = False
                                for command in result["jobsframework_metacommands"]:
                                    if "setTimeout" in command:
                                        job.change_visibility(command["setTimeout"])
                                    elif "defer" in command:
                                        status = get_status(ticket)
                                        status["status"] = "queued"
                                        status["meta"]["lastDeferredTime"] = str(datetime.now())
                                        set_status(ticket, status)
                                        log("Deferred work on ticket {}.".format(ticket), worker_id)
                                        defer = True
                                    elif "resubmit" in command:
                                        submit_job(req)
                                        log("Resubmitted job that was in ticket {}.".format(ticket), worker_id)
                                        stop = True
                                if stop:
                                    JobsQueue.delete_message(job)
                                    continue
                                if defer:
                                    continue

                        elif endpoint in shape_logic:
                            convert(query_args)
                            query_args = ValidatorProxy(query_args)
                            result = shape_logic[endpoint](query_args)
                            if endpoint == 'aggregate-point-data' and query_args.data.get('data_type') != 'csv':
                                result = convert_result_geoms(result)

                        elif endpoint in etl_logic:
                            if endpoint in ('update_weather', 'update_metar'):
                                result = etl_logic[endpoint]()
                            else:
                                result = etl_logic[endpoint](query_args)

                        else:
                            raise ValueError("Attempting to send a job to an "
                                             "invalid endpoint ->> {}"
                                             .format(endpoint))

                        set_result(ticket, result)
                        status = get_status(ticket)
                        status["status"] = "success"
                        status["meta"]["endTime"] = str(datetime.now())
                        set_status(ticket, status)

                        log("Finished work on ticket {}.".format(ticket), worker_id)
                        JobsQueue.delete_message(job)
                    except Exception as e:
                        traceback.print_exc()

                        status = get_status(ticket)
                        if status['meta'].get('tries'):
                            status['meta']['tries'] += 1
                        else:
                            status['meta']['tries'] = 1

                        # Try failed jobs twice
                        if status["meta"]["tries"] > 2:
                            error_msg = "{} errored with {}.".format(ticket, e)
                            set_ticket_error(status, ticket, error_msg, worker_id)
                            JobsQueue.delete_message(job)
                        else:
                            status["status"] = "queued"
                            status["meta"]["lastDeferredTime"] = str(datetime.now())
                            log("ERROR: Ticket {} errored with: {}...retrying.".format(ticket, e), worker_id)
                            set_status(ticket, status)

                    finally:
                        deregister_job(birthtime, worker_id)
                        worker_boss['active_worker_count'] -= 1
                        update_instance_protection(worker_boss, autoscaling_client)

                else:
                    # No work! Idle for a bit to save compute cycles.
                    time.sleep(wait_interval)

        log("Exited run loop. Goodbye!", worker_id)
        deregister_worker(worker_id)

    log("RUNNING FROM DIRECTORY: {}".format(os.getcwd()), "WORKER BOSS")

    threads = []
    for i in range(worker_threads):
        t = threading.Thread(target=worker, name="plenario-worker-thread")
        t.daemon = False
        threads.append(t)
        t.start()

    # Join threads back into main loop
    looper = 0
    while len(threads) > 0:
        looper = (looper + 1) % len(threads)
        t = threads[looper]
        t.join(5)
        if not t.is_alive():
            threads.pop(looper)
            # If threads exit prematurely, then replace them.
            if worker_boss['do_work']:
                log("ERROR: A WORKER DIED PREMATURELY! REPLACING.".format(len(threads)), "WORKER BOSS")
                print("====================== WORKER BOSS STATUS: ======================")
                print check_output(["cat", "/proc/{}/status".format(os.getpid())])
                print("====================== WORKER BOSS MEMDUMP: ======================")
                print check_output(["cat", "/proc/{}/maps".format(os.getpid())])
                worker_boss['active_worker_count'] -= 1
                t = threading.Thread(target=worker, name="plenario-worker-thread")
                t.daemon = False
                threads.append(t)
                t.start()

    session.close()
    log("All workers have exited.", "WORKER BOSS")
