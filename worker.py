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

import plenario.database
session = plenario.database.session

worker_threads = 4
max_wait_interval = 15

if __name__ == "__main__":

    import datetime
    import time
    import signal
    import boto.sqs
    import threading
    import traceback
    import random
    from collections import namedtuple
    from plenario.settings import AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION_NAME, JOBS_QUEUE
    from plenario.api.point import _timeseries, _detail, _detail_aggregate, _meta, _grid, _datadump_cleanup, _datadump_manager, _datadump
    from plenario.api.jobs import get_status, set_status, get_request, set_result
    from plenario.api.shape import _aggregate_point_data, _export_shape
    from plenario.api.response import convert_result_geoms
    from plenario.api.validator import convert
    from plenario.tasks import add_dataset, delete_dataset, update_dataset
    from plenario.tasks import add_shape, update_shape, delete_shape
    from plenario.utils.name_generator import generate_name
    from flask import Flask

    do_work = True

    ValidatorProxy = namedtuple("ValidatorProxy", ["data"])

    def log(msg, worker_id):
        # The constant opening and closing is meh, I know. But I'm feeling lazy
        # right now.
        logfile = open('/opt/python/log/worker.log', "a")
        logfile.write("{} - Worker {}: {}\n".format(datetime.datetime.now(), worker_id.ljust(24), msg))
        logfile.close()


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


    def worker():
        worker_id = generate_name()

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
                                         if key not in ['point_date', 'hash', 'geom']} for row in _detail(args)],
                # /datasets?<args>
                'meta': lambda args: _meta(args),
                # /fields/<dataset>
                'fields': lambda args: _meta(args),
                # /grid?<args>
                'grid': lambda args: _grid(args),
                'datadump': lambda args: _datadump_manager(args),
                'datadump_work': lambda args: _datadump(args),
                # Health endpoint.
                'ping': lambda args: {'hello': 'from worker {}'.format(worker_id)},
                # Utility tasks
                'datadump_cleanup': lambda args: _datadump_cleanup(args)
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
            }

            log("Hello! I'm ready for anything.", worker_id)

            while do_work:
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
                        log("Job does not contain a ticket! Removing.", worker_id)
                        JobsQueue.delete_message(job)
                        continue

                    log("Received job with ticket {}.".format(ticket), worker_id)

                    try:
                        status = get_status(ticket)
                        status["status"]
                        status["meta"]
                    except Exception as e:
                        log("Job is malformed ({}). Removing.".format(e), worker_id)
                        JobsQueue.delete_message(job)
                        continue

                    if status["status"] != "queued":
                        log("Job has already been started. Skipping.", worker_id)
                        continue

                    status["status"] = "processing"
                    if "lastDeferredTime" in status["meta"]:
                        status["meta"]["lastResumeTime"] = str(datetime.datetime.now())
                    else:
                        status["meta"]["startTime"] = str(datetime.datetime.now())
                    if "workers" not in status["meta"]:
                        status["meta"]["workers"] = []
                    status["meta"]["workers"].append(worker_id)
                    set_status(ticket, status)

                    log("Starting work on ticket {}.".format(ticket), worker_id)

                    # =========== Do work on query =========== #
                    try:
                        req = get_request(ticket)
                        endpoint = req['endpoint']
                        query_args = req['query']

                        if endpoint in endpoint_logic:

                            # Add worker metadata
                            query_args["jobsframework_ticket"] = ticket
                            query_args["jobsframework_workerid"] = worker_id

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
                                if defer:
                                    continue

                        elif endpoint in shape_logic:
                            convert(query_args)
                            query_args = ValidatorProxy(query_args)
                            result = shape_logic[endpoint](query_args)
                            if endpoint == 'aggregate-point-data' and query_args.data.get('data_type') != 'csv':
                                result = convert_result_geoms(result)

                        elif endpoint in etl_logic:

                            log("Ticket {} uses endpoint {}.".format(ticket, endpoint), worker_id)
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
                        status["status"] = "error"
                        status["meta"]["endTime"] = str(datetime.datetime.now())
                        log("Ticket {} errored with: {}.".format(ticket, e), worker_id)
                        set_status(ticket, status)
                        set_result(ticket, {"error": str(e)})
                        JobsQueue.delete_message(job)
                        traceback.print_exc()

                else:
                    # No work! Idle for a bit to save compute cycles.
                    # This interval is random in order to stagger workers
                    idle = random.randrange(max_wait_interval)
                    log("Ho hum nothing to do. Idling for {} seconds.".format(idle), worker_id)
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
