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

import boto3
import plenario.database
import traceback

from plenario.models import Workers
import datetime

session = plenario.database.session

worker_threads = 8
wait_interval = 1
job_timeout = 3600


# ======================= Worker Utilities ========================
def log(msg, worker_id):
    logfile = open('/opt/python/log/worker.log', "a")
    logfile.write("{} - Worker {}: {}\n".format(datetime.datetime.now(), worker_id.ljust(24), msg))
    logfile.close()

# =========================== The Worker Process ===============================
if __name__ == "__main__":

    import time
    import signal
    import boto.sqs
    import threading
    import os
    from subprocess import check_output
    from collections import namedtuple
    from subprocess import check_output
    from plenario.settings import AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION_NAME, JOBS_QUEUE, INSTANCE_ID, AUTOSCALING_GROUP
    from plenario.api.point import _timeseries, _detail, _detail_aggregate, _meta, _grid, _datadump
    from plenario.api.jobs import get_status, set_status, get_request, set_result, submit_job
    from plenario.api.shape import _aggregate_point_data, _export_shape
    from plenario.api.response import convert_result_geoms
    from plenario.api.validator import convert
    from plenario.tasks import add_dataset, delete_dataset, update_dataset
    from plenario.tasks import add_shape, update_shape, delete_shape
    from plenario.tasks import update_weather, frequency_update
    from plenario.utils.name_generator import generate_name
    from flask import Flask

    do_work = True

    ValidatorProxy = namedtuple("ValidatorProxy", ["data"])

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
    JobsQueue = conn.get_queue(JOBS_QUEUE)

    # While the worker is performing a task it pulled
    # off SQS, protect its host EC2 instance from being
    # scaled in.
    autoscaling_client = boto3.client('autoscaling', region_name=AWS_REGION_NAME,
          aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)

    # Used in deciding when to remove aforementioned
    # scale-in protection. A count of the workers
    # actively performing a job.
    active_worker_count = 0

    protected = False

    def update_instance_protection():
        global protected, do_work
        try:
            if active_worker_count > 0 and not protected:
                log("INSTANCE PROTECTION ENABLED", "WORKER BOSS")
                autoscaling_client.set_instance_protection(
                    InstanceIds=[INSTANCE_ID],
                    AutoScalingGroupName=AUTOSCALING_GROUP,
                    ProtectedFromScaleIn=True
                )
                protected = True
            elif active_worker_count <= 0 and protected:
                log("INSTANCE PROTECTION DISABLED", "WORKER BOSS")
                autoscaling_client.set_instance_protection(
                    InstanceIds=[INSTANCE_ID],
                    AutoScalingGroupName=AUTOSCALING_GROUP,
                    ProtectedFromScaleIn=False
                )
                protected = False
        except Exception as e:
            if "is not in InService or EnteringStandby or Standby" in e:
                log("Could not apply INSTANCE PROTECTION: INSTANCE IS TERMINATING!", "WORKER BOSS")
                log("Stopping work in the face of imminent termination...", "WORKER BOSS")
                do_work = False
            log("Could not apply INSTANCE PROTECTION: {}".format(e), "WORKER BOSS")


    def check_in(birthtime, worker_id):
        log("INFO: Hi! Just checking in.", worker_id)
        try:
            session.query(Workers).filter(Workers.name == worker_id).one().check_in()
            session.commit()
        except Exception as e:
            session.rollback()
            if session.query(Workers).filter(Workers.name == worker_id).count() == 0:
                register_worker(birthtime, worker_id)
            else:
                traceback.print_exc()
                log("ERROR: Problem updating worker registration: {}".format(e), worker_id)


    def register_job(ticket, birthtime, worker_id):
        log("INFO: Registering job {}.".format(ticket), worker_id)
        global active_worker_count
        check_in(birthtime, worker_id)
        try:
            session.query(Workers).filter(Workers.name == worker_id).one().register_job(ticket)
            session.commit()
        except Exception as e:
            session.rollback()
            if session.query(Workers).filter(Workers.name == worker_id).count() == 0:
                register_worker(birthtime, worker_id)
            else:
                traceback.print_exc()
                log("ERROR: Problem updating worker registration: {}".format(e), worker_id)


    def deregister_job(birthtime, worker_id):
        log("INFO: Deregistering job.", worker_id)
        global active_worker_count
        check_in(birthtime, worker_id)
        try:
            session.query(Workers).filter(Workers.name == worker_id).one().deregister_job()
            session.commit()
        except Exception as e:
            session.rollback()
            if session.query(Workers).filter(Workers.name == worker_id).count() == 0:
                register_worker(birthtime, worker_id)
            else:
                traceback.print_exc()
                log("ERROR: Problem updating worker registration: {}".format(e), worker_id)


    def register_worker(birthtime, worker_id):
        log("INFO: Registering worker.", worker_id)
        try:
            session.add(Workers(worker_id, int(birthtime)))
            session.commit()
        except Exception as e:
            traceback.print_exc()
            session.rollback()
            log("ERROR: Problem updating worker registration: {}".format(e), worker_id)


    def deregister_worker(worker_id):
        try:
            session.query(Workers).filter(Workers.name == worker_id).delete()
            session.commit()
        except Exception as e:
            traceback.print_exc()
            session.rollback()
            log("Problem updating worker registration: {}".format(e), worker_id)


    def worker():
        worker_id = generate_name()
        birthtime = time.time()

        global active_worker_count

        while do_work:
            try:
                # Holds the methods which perform the work requested by an incoming job.
                app = Flask(__name__)
                with app.app_context():

                    endpoint_logic = {
                        # /timeseries?<args>
                        'timeseries': lambda args: _timeseries(args),
                        # /detail-aggregate?<args>
                        'detail-aggregate': lambda args: _detail_aggregate(args),
                        # /detail?<args>
                        # emulating row-removal features of _detail_response. Not very DRY, but it's the cleanest option.
                        'detail': lambda args: [{key: row[key] for key in row.keys()
                                                 if key not in ['point_date', 'hash', 'geom']} for row in
                                                _detail(args)],
                        # /datasets?<args>
                        'meta': lambda args: _meta(args),
                        # /fields/<dataset>
                        'fields': lambda args: _meta(args),
                        # /grid?<args>
                        'grid': lambda args: _grid(args),
                        'datadump': lambda args: _datadump(args),
                        # Health endpoint.
                        'ping': lambda args: {'hello': 'from worker {}'.format(worker_id)}
                    }

                    shape_logic = {
                        # /shapes/<shape>?<args>
                        'export-shape': lambda args: _export_shape(args),
                        # /shapes/<dataset>/<shape>?<args>
                        'aggregate-point-data': lambda args: [{key: row[key] for key in row.keys()
                                                               if key not in ['hash', 'ogc_fid']}
                                                              for row in _aggregate_point_data(args)]
                    }

                    etl_logic = {
                        'add_dataset': lambda args: add_dataset(args),
                        'update_dataset': lambda args: update_dataset(args),
                        'delete_dataset': lambda args: delete_dataset(args),
                        'add_shape': lambda args: add_shape(args),
                        'update_shape': lambda args: update_shape(args),
                        'delete_shape': lambda args: delete_shape(args),
                        "update_weather": lambda: update_weather(),
                        "frequency_update": lambda args: frequency_update(args)
                    }

                    log("Hello! I'm ready for anything.", worker_id)
                    register_worker(birthtime, worker_id)

                    throttle = 5
                    while do_work:

                        if throttle < 0:
                            check_in(birthtime, worker_id)
                            throttle = 5
                        throttle -= 1

                        response = JobsQueue.get_messages(message_attributes=["ticket"])
                        if len(response) > 0:
                            job = response[0]
                            body = job.get_body()
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
                                status["status"]
                                status["meta"]
                            except Exception as e:
                                log("ERROR: Job is malformed ({}). Removing.".format(e), worker_id)
                                JobsQueue.delete_message(job)
                                continue

                            # Handle orphaned jobs
                            if status["status"] == "processing" and \
                                    ((not status["meta"].get("lastStartTime") and
                                                  (datetime.datetime.now()-
                                                       datetime.datetime.strptime(status["meta"]["startTime"],
                                                        "%Y-%m-%d %H:%M:%S.%f")).total_seconds() > job_timeout) or
                                    (status["meta"].get("lastStartTime") and
                                                  (datetime.datetime.now()-
                                                       datetime.datetime.strptime(status["meta"]["lastStartTime"],
                                                        "%Y-%m-%d %H:%M:%S.%f")).total_seconds() > job_timeout)):

                                status["meta"]["tries"] = status["meta"]["tries"] + 1 \
                                    if status["meta"].get("tries") else 1

                                # Only try orphaned jobs again once
                                if status["meta"]["tries"] > 1:
                                    status["status"] = "error"
                                    status["meta"]["endTime"] = str(datetime.datetime.now())
                                    log("ERROR: ALERT! Ticket {} has been stalling workers. Removing.".format(ticket), worker_id)
                                    set_status(ticket, status)
                                    set_result(ticket, {"error": "Job stalled."})
                                    JobsQueue.delete_message(job)
                                    deregister_job(birthtime, worker_id)
                                    continue
                                else:
                                    log("WARNING: Ticket {} has been orphaned...retrying.".format(ticket), worker_id)
                                    status["meta"]["lastDeferredTime"] = str(datetime.datetime.now())
                                    set_status(ticket, status)
                                traceback.print_exc()

                            # Standard job mutex
                            elif status["status"] != "queued":
                                log("Job has already been started. Skipping.", worker_id)
                                continue

                            active_worker_count += 1
                            update_instance_protection()
                            if not do_work:
                                deregister_job(birthtime, worker_id)
                                continue
                            register_job(ticket, birthtime, worker_id)

                            status["status"] = "processing"
                            if "lastDeferredTime" in status["meta"]:
                                status["meta"]["lastResumeTime"] = str(datetime.datetime.now())
                            else:
                                status["meta"]["startTime"] = str(datetime.datetime.now())
                            if "workers" not in status["meta"]:
                                status["meta"]["workers"] = []
                            status["meta"]["workers"].append(worker_id)
                            set_status(ticket, status)

                            # =========== Do work on query =========== #
                            try:
                                log("Starting work on ticket {}.".format(ticket), worker_id)

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

                                    log("Ticket {} uses endpoint {}.".format(ticket, endpoint), worker_id)

                                    # log("worker.query_args: {}".format(query_args), worker_id)
                                    # log("worker.req: {}".format(req), worker_id)
                                    # print "{} worker.query_args: {}".format(ticket, query_args)

                                    result = endpoint_logic[endpoint](query_args)
                                    # pprint("worker.result: {}".format(result))

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
                                                status["meta"]["lastDeferredTime"] = str(datetime.datetime.now())
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
                                status["meta"]["endTime"] = str(datetime.datetime.now())
                                set_status(ticket, status)

                                log("Finished work on ticket {}.".format(ticket), worker_id)
                                JobsQueue.delete_message(job)
                            except Exception as e:
                                status = get_status(ticket)
                                status["meta"]["tries"] = status["meta"]["tries"] + 1 \
                                    if status["meta"].get("tries") else 1

                                # Try failed jobs twice
                                if status["meta"]["tries"] > 2:
                                    status["status"] = "error"
                                    status["meta"]["endTime"] = str(datetime.datetime.now())
                                    log("ERROR: Ticket {} errored with: {}.".format(ticket, e), worker_id)
                                    set_status(ticket, status)
                                    set_result(ticket, {"error": str(e)})
                                    JobsQueue.delete_message(job)
                                else:
                                    status["status"] = "queued"
                                    status["meta"]["lastDeferredTime"] = str(datetime.datetime.now())
                                    log("ERROR: Ticket {} errored with: {}...retrying.".format(ticket, e), worker_id)
                                    set_status(ticket, status)
                                traceback.print_exc()
                            finally:
                                active_worker_count -= 1
                                update_instance_protection()
                                deregister_job(birthtime, worker_id)

                        else:
                            # No work! Idle for a bit to save compute cycles.
                            #log("Ho hum nothing to do. Idling for {} seconds.".format(wait_interval), worker_id)
                            time.sleep(wait_interval)

                    deregister_worker(worker_id)

            except Exception as e:
                traceback.print_exc()
                log("ERROR: {}. Retrying in 5 seconds.".format(e), worker_id)
                time.sleep(5)
        log("Exited run loop. Goodbye!", worker_id)



    # Each container should only have one worker.py running at all times.
    # while len(check_output("pgrep -f worker.py", shell=True).split("\n"))-1 > 0:
    #     print "There is already a worker process. Waiting for it to exit..."
    #     time.sleep(5)

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
            if do_work:
                log("ERROR: A WORKER DIED PREMATURELY! REPLACING.".format(len(threads)), "WORKER BOSS")
                print("====================== WORKER BOSS STATUS: ======================")
                print check_output(["cat", "/proc/{}/status".format(os.getpid())])
                print("====================== WORKER BOSS MEMDUMP: ======================")
                print check_output(["cat", "/proc/{}/maps".format(os.getpid())])
                active_worker_count -= 1
                t = threading.Thread(target=worker, name="plenario-worker-thread")
                t.daemon = False
                threads.append(t)
                t.start()

                # TODO: Make active_worker_count account for workers which may die in the middle of the run loop.

    session.close()
    log("All workers have exited.", "WORKER BOSS")
