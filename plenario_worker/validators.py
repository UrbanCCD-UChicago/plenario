from datetime import datetime
from plenario.api.jobs import get_status


def available_jobs(response):
    return len(response) > 0


def has_plenario_job(response):
    job = response[0]
    body = job.get_body()
    return body == "plenario_job"


def has_valid_ticket(response):
    job = response[0]
    try:
        ticket = str(job.message_attributes["ticket"]["string_value"])
        status = get_status(ticket)
        assert status["status"] is not None
        assert status["meta"] is not None
        return True
    except (AssertionError, KeyError):
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
