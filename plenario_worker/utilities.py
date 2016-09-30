"""utilities: helper functions that serve to monitor the health and activity
of the worker threads."""

import boto3
import requests
import traceback
import warnings
from datetime import datetime
from plenario.database import session
from plenario.models import Workers


def get_ec2_instance_id():
    """Retrieve the instance id for the currently running EC2 instance. If
    the host machine is not an EC2 instance or is for some reason unable
    to make requests, return None.

    :returns: (str) id of the current EC2 instance
              (None) if the id could not be found"""

    instance_id_url = "http://169.254.169.254/latest/meta-data/instance-id"
    try:
        return requests.get(instance_id_url).text
    except requests.ConnectionError:
        print "Could not find EC2 instance id..."
        return None


INSTANCE_ID = get_ec2_instance_id()


# TODO: Test get_autoscaling_group
def get_autoscaling_group():
    """Retrieve the autoscaling group name of the current instance. If
    the host machine is not an EC2 instance, not subject to autoscaling,
    or unable to make requests, return None.

    :returns: (str) id of the current autoscaling group
              (None) if the id could not be found"""

    autoscaling_client = boto3.client("autoscaling")
    try:
        return autoscaling_client.describe_autoscaling_instances(
            InstanceIds=[INSTANCE_ID]
        )["AutoscalingInstances"][0]["AutoscalingGroupName"]
    except Exception as exc:
        print "Could not find autoscaling group..."
        raise exc


AUTOSCALING_GROUP = get_autoscaling_group()


def log(msg, worker_id):
    try:
        logfile = open('/opt/python/log/worker.log', "a")
    except IOError:
        warnings.warn("Failed to write to /opt/python/log/worker.log - "
                      "writing to current directory.", RuntimeWarning)
        logfile = open("./worker.log", "a")        
    logfile.write("{} - Worker {}: {}\n".format(datetime.now(), worker_id.ljust(24), msg))
    logfile.close()


def check_in(birthtime, worker_id):
    log("INFO: Checking in.", worker_id)
    try:
        session.query(Workers).filter(Workers.name == worker_id).one().check_in()
        session.commit()
    except Exception as e:
        traceback.print_exc()
        session.rollback()
        if session.query(Workers).filter(Workers.name == worker_id).count() == 0:
            register_worker(birthtime, worker_id)
        else:
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
    log("INFO: Deregistering worker.", worker_id)
    try:
        session.query(Workers).filter(Workers.name == worker_id).delete()
        session.commit()
    except Exception as e:
        traceback.print_exc()
        session.rollback()
        log("Problem updating worker registration: {}".format(e), worker_id)


def register_worker_job_status(ticket, birthtime, worker_id):
    log("INFO: Registering job for worker {}.", worker_id)
    check_in(birthtime, worker_id)
    try:
        session.query(Workers).filter(Workers.name == worker_id).one().register_job(ticket)
        session.commit()
    except Exception as e:
        session.rollback()
        if session.query(Workers).filter(Workers.name == worker_id).count() == 0:
            register_worker(birthtime, worker_id)
        else:
            log("ERROR: Problem updating worker registration: {}".format(e), worker_id)
            traceback.print_exc()


def deregister_worker_job_status(birthtime, worker_id):
    log("INFO: Deregistering job for worker {}.", worker_id)
    check_in(birthtime, worker_id)
    try:
        session.query(Workers).filter(Workers.name == worker_id).one().deregister_job()
        session.commit()
    except Exception as e:
        session.rollback()
        if session.query(Workers).filter(Workers.name == worker_id).count() == 0:
            register_worker(birthtime, worker_id)
        else:
            log("ERROR: Problem updating worker registration: {}".format(e), worker_id)


def update_instance_protection(worker_boss, autoscaling_client):

    try:
        if worker_boss.active_worker_count > 0 and not worker_boss.protected:
            log("INSTANCE PROTECTION ENABLED", "WORKER BOSS")
            autoscaling_client.set_instance_protection(
                InstanceIds=[INSTANCE_ID],
                AutoScalingGroupName=AUTOSCALING_GROUP,
                ProtectedFromScaleIn=True
            )
            worker_boss.protected = True
        elif worker_boss.active_worker_count <= 0 and worker_boss.protected:
            log("INSTANCE PROTECTION DISABLED", "WORKER BOSS")
            autoscaling_client.set_instance_protection(
                InstanceIds=[INSTANCE_ID],
                AutoScalingGroupName=AUTOSCALING_GROUP,
                ProtectedFromScaleIn=False
            )
            worker_boss.protected = False

    except Exception as e:
        if "is not in InService or EnteringStandby or Standby" in e:
            log("Could not apply INSTANCE PROTECTION: {}".format(e), "WORKER BOSS")
            log("INSTANCE TERMINATING!", "WORKER BOSS")
            worker_boss.do_work = False
        else:
            log("Could not apply INSTANCE PROTECTION: {}".format(e), "WORKER BOSS")


def increment_job_trial_count(job_status):

    if job_status["meta"].get("tries"):
        job_status["meta"]["tries"] += 1
    else:
        job_status["meta"]["tries"] = 0
    return job_status
