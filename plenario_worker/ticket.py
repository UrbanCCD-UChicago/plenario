from datetime import datetime
from plenario.api.jobs import set_status, set_result
from plenario_worker.utilities import log


def set_ticket_error(status_dict, ticket_id, error_msg, worker_id):
    log("TICKET ERROR: {}".format(error_msg), worker_id)

    status_dict["status"] = "error"
    status_dict["meta"]["endTime"] = str(datetime.now())

    set_status(ticket_id, status_dict)
    set_result(ticket_id, {"error": str(error_msg)})
