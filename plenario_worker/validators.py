from datetime import datetime
from plenario.api.jobs import get_status


def has_valid_ticket(job):
    try:
        print "has_valid_ticket.job.message_attributes: {}".format(job.message_attributes)
        ticket = str(job.message_attributes["ticket"]["StringValue"])
        status = get_status(ticket)
        assert status is not None
        assert status["status"] is not None
        assert status["meta"] is not None
        return True
    except (AssertionError, KeyError, TypeError):
        return False


def is_job_status_orphaned(status, job_timeout):
    # Booleans about the jobs state to determine if it is a valid
    # job to work on.
    is_processing = status["status"] == "processing"
    is_deferred = status["meta"].get("lastStartTime") is not None

    if is_processing:
        is_expired = (datetime.now() - datetime.strptime(status["meta"]["startTime"], "%Y-%m-%d %H:%M:%S.%f")).total_seconds() > job_timeout
        deferral_expired = (datetime.now() - datetime.strptime(status["meta"]["lastResumeTime"], "%Y-%m-%d %H:%M:%S.%f")).total_seconds() > job_timeout
        return (not is_deferred and is_expired) or (is_deferred and deferral_expired)

    return False
