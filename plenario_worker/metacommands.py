from datetime import datetime
from plenario.api.jobs import set_status, get_status, submit_job
from plenario_worker.utilities import log


def process_metacommands(result, job, ticket, worker_id, request, jobs_queue):
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
                submit_job(request)
                log("Resubmitted job that was in ticket {}.".format(ticket), worker_id)
                stop = True

        if stop:
            jobs_queue.delete_message(job)
            return "STOP"
        if defer:
            return "DEFER"
