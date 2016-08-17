from datetime import datetime
from plenario.api.jobs import set_status, set_result, get_status
from plenario_worker.utilities import log


def set_ticket_error(status_dict, ticket_id, error_msg, worker_id):
    log("TICKET ERROR: {}".format(error_msg), worker_id)

    status_dict["status"] = "error"
    status_dict["meta"]["endTime"] = str(datetime.now())

    set_status(ticket_id, status_dict)
    set_result(ticket_id, {"error": str(error_msg)})


def set_ticket_success(ticket, result):
    set_result(ticket, result)
    status = get_status(ticket)
    status["status"] = "success"
    status["meta"]["endTime"] = str(datetime.now())
    set_status(ticket, status)


def set_ticket_queued(job_status, ticket, msg, worker_id):
    job_status["status"] = "queued"
    job_status["meta"]["lastDeferredTime"] = str(datetime.now())
    log("ERROR: Ticket {} errored with: {}...retrying.".format(ticket, msg), worker_id)
    set_status(ticket, job_status)
