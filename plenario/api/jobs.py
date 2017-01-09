import pickle

from flask import jsonify

from plenario.database import Base, app_engine as engine
from plenario.utils.helpers import reflect


def get_job(ticket: str):

    celery_taskmeta = reflect("celery_taskmeta", Base.metadata, engine)
    query = celery_taskmeta.select().where(celery_taskmeta.c.task_id == ticket)
    job_meta = dict(query.execute().first().items())
    job_meta["result"] = pickle.loads(job_meta["result"])

    return job_meta


def make_job_response(endpoint, validated_query):

    msg = "This feature, enabled by the jobs=true flag, is currently " \
          "undergoing maintenance, we apologize for any inconvenience."
    return jsonify({"unavailable": msg})

