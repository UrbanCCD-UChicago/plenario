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

from collections import defaultdict, namedtuple
from datetime import datetime
from flask import Flask

# Exists out here because of its use in session.
from plenario.database import session

worker_threads = 4
wait_interval = 5
job_timeout = 3600

if __name__ == "__main__":

    from plenario.api.jobs import get_status, set_status, get_request
    from plenario.api.response import convert_result_geoms
    from plenario.api.validator import convert

    from plenario_worker.clients import autoscaling_client, job_queue
    from plenario_worker.endpoints import endpoint_logic, etl_logic, shape_logic
    from plenario_worker.metacommands import process_metacommands
    from plenario_worker.names import generate_name
    from plenario_worker.ticket import set_ticket_error, set_ticket_success, set_ticket_queued
    from plenario_worker.utilities import deregister_worker_job_status, register_worker_job_status
    from plenario_worker.utilities import deregister_worker, register_worker
    from plenario_worker.utilities import check_in, log, update_instance_protection
    from plenario_worker.validators import has_valid_ticket, is_job_status_orphaned
    from plenario_worker.worker_boss import WorkerBoss

    # Holds the boolean which determines if the workers are running or not.
    worker_boss = WorkerBoss()
    # Listen for termination signals telling us to break the worker run loop.
    signal.signal(signal.SIGINT, worker_boss.stop_working)
    signal.signal(signal.SIGTERM, worker_boss.stop_working)
    # Emulate a ValidatorResult, used to play nice with some endpoint logic.
    ValidatorProxy = namedtuple("ValidatorProxy", ["data"])
    # Need this to perform operations which require an application context.
    app = Flask(__name__)

    def worker(worker_id):

        birthtime = time.time()

        log("Hello! I'm ready for anything.", worker_id)
        register_worker(birthtime, worker_id)

        with app.app_context():

            while worker_boss.do_work:
                # Report to the worker boss.
                worker_boss.workers[worker_id] = "alive"
                # Report to the status page.
                check_in(birthtime, worker_id)
                # Poll Amazon SQS for messages containing a job.
                response = job_queue.receive_messages(MessageAttributeNames=["ticket"])
                # Grab the first message as the job to be considered.
                job = response[0] if len(response) > 0 else None

                # Run checks on the validity of the message. If any of the
                # checks fail, do a tiny amount of work then skip the loop.
                if not job:
                    time.sleep(wait_interval)
                    continue
                elif not job.body == "plenario_job":
                    log("Message is not a Plenario Job. Skipping.", worker_id)
                    continue
                elif not has_valid_ticket(job):
                    log("ERROR: Job does not contain a valid ticket! Removing.", worker_id)
                    job.delete()
                    continue

                # All checks passed, this is a valid ticket.
                ticket = str(job.message_attributes["ticket"]["StringValue"])
                status = get_status(ticket)
                log("Received job with ticket {}.".format(ticket), worker_id)

                # Keep track of the last 1000 tickets we've seen, these counts
                # help to determine how many times to retry jobs if they fail
                if len(worker_boss.tickets) > 1000:
                    worker_boss.tickets = defaultdict(int)
                worker_boss.tickets[ticket] += 1

                # Now we have to determine if the job itself is a valid job to
                # perform, taking into consideration whether it has been
                # orphaned or has undergone too many retries.
                is_processing = status["status"] == "processing"
                is_orphaned = is_job_status_orphaned(status, job_timeout)

                # Check if the job was an orphan (meaning that the parent worker
                # process died and failed to complete it). If it was orphaned,
                # give it another try.
                if is_processing and is_orphaned:
                    log("Retrying orphan ticket {}".format(ticket), worker_id)
                    status["meta"]["lastDeferredTime"] = str(datetime.now())
                    set_status(ticket, status)
                # If the job isn't an orphan, check its status to make sure no
                # other worker has started work on it.
                elif status["status"] != "queued":
                    log("Job has already been started. Skipping.", worker_id)
                    continue

                register_worker_job_status(ticket, birthtime, worker_id)
                worker_boss.active_worker_count += 1
                # Once we have established that both the ticket and the job
                # are valid and able to be worked upon, give the EC2 instance
                # that contains this worker scale-in protection.
                update_instance_protection(worker_boss, autoscaling_client)
                # There is a chance that the do_work switch is False due to
                # an immenent instance termination.
                if not worker_boss.do_work:
                    deregister_worker_job_status(birthtime, worker_id)
                    worker_boss.active_worker_count -= 1
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

                try:
                    log("Starting work on ticket {}.".format(ticket), worker_id)

                    req = get_request(ticket)
                    endpoint = req['endpoint']
                    query_args = req['query']

                    worker_boss.workers[worker_id] = "working on {}".format(endpoint)

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

                        # Metacommands enable workers to modify a job's priority through a variety
                        # of methods (deferral, set_timeout, resubmission). Metacommands are recieved
                        # from work done in the endpoint logics.
                        metacommand = process_metacommands(result, job, ticket, worker_id, req, job_queue)
                        if metacommand == "STOP":
                            job.delete()
                            continue
                        if metacommand == "DEFER":
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

                    # By this point, we have successfully completed a task.
                    # Update the status meta information to indicate so.
                    set_ticket_success(ticket, result)
                    # Cleanup the leftover SQS message.
                    job.delete()

                    log("Finished work on ticket {}.".format(ticket), worker_id)

                except Exception as e:
                    traceback.print_exc()

                    times_ticket_was_seen = worker_boss.tickets[ticket]
                    if times_ticket_was_seen < 3:
                        set_ticket_queued(status, ticket, str(e), worker_id)
                    else:
                        error_msg = "{} errored with {}.".format(ticket, e)
                        set_ticket_error(status, ticket, error_msg, worker_id)
                        job.delete()

                finally:
                    worker_boss.workers[worker_id] = "alive"
                    worker_boss.active_worker_count -= 1
                    update_instance_protection(worker_boss, autoscaling_client)
                    deregister_worker_job_status(birthtime, worker_id)
                    time.sleep(wait_interval)

        log("Exited run loop. Goodbye!", worker_id)
        deregister_worker(worker_id)

    log("RUNNING FROM DIRECTORY: {}".format(os.getcwd()), "WORKER BOSS")

    threads = {}
    # Start up all the threads.
    for i in range(worker_threads):
        # Give it a memorable name.
        worker_name = generate_name()
        # Name the thread and pass the name to the worker.
        t = threading.Thread(target=worker, name=worker_name, args=[worker_name])
        t.daemon = False
        threads[worker_name] = t
        t.start()

    # Stay busy on the main thread by reporting on overall worker health
    while worker_boss.do_work:
        # Records dead threads for the following log report
        worker_boss.check_on_worker_threads(threads)
        # Report information about this process to /opt/python/log/worker.log
        worker_boss.report()
        # Revive dead workers if we come across any during our check
        worker_boss.rescue(threads, worker)
        time.sleep(10)
    # When the termination signal is received, kindly wait for all the
    # threads to finish working by calling thread.join()
    worker_boss.terminate(threads)

    session.close()
    log("All workers have exited.", "WORKER BOSS")
