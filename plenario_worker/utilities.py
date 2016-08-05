"""utilities: helper functions that serve to monitor the health and activity
of the worker threads."""

import traceback
from datetime import datetime
from plenario.database import session
from plenario.models import Workers
from plenario.settings import AUTOSCALING_GROUP, INSTANCE_ID


def log(msg, worker_id):
    logfile = open('/opt/python/log/worker.log', "a")
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
    update_instance_protection()


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
            traceback.print_exc()
    update_instance_protection()


def update_instance_protection(worker_boss, autoscaling_client):

    try:
        if worker_boss['active_worker_count'] > 0 and not worker_boss['protected']:
            log("INSTANCE PROTECTION ENABLED", "WORKER BOSS")
            autoscaling_client.set_instance_protection(
                InstanceIds=[INSTANCE_ID],
                AutoScalingGroupName=AUTOSCALING_GROUP,
                ProtectedFromScaleIn=True
            )
            worker_boss['protected'] = True
        elif worker_boss['active_worker_count'] <= 0 and worker_boss['protected']:
            log("INSTANCE PROTECTION DISABLED", "WORKER BOSS")
            autoscaling_client.set_instance_protection(
                InstanceIds=[INSTANCE_ID],
                AutoScalingGroupName=AUTOSCALING_GROUP,
                ProtectedFromScaleIn=False
            )
            worker_boss['protected'] = False

    except Exception as e:
        if "is not in InService or EnteringStandby or Standby" in e:
            log("Could not apply INSTANCE PROTECTION: {}".format(e), "WORKER BOSS")
            log("INSTANCE TERMINATING!")
            worker_boss['do_work'] = False
        else:
            log("Could not apply INSTANCE PROTECTION: {}".format(e), "WORKER BOSS")
