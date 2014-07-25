import os
from urlparse import urlparse
from plenario.celery_app import celery_app
from plenario.models import MetaTable
from plenario.database import task_session as session
from plenario.utils.etl import PlenarioETL
# from raven.handlers.logging import SentryHandler
# from raven.conf import setup_logging

# handler = SentryHandler(os.environ['CELERY_SENTRY_URL'])
# setup_logging(handler)

@celery_app.task
def add_dataset(source_url):
    md = session.query(MetaTable).get(source_url)
    etl = PlenarioETL(md.as_dict())
    etl.add()
    return 'yay'

@celery_app.task
def update_dataset(source_url):
    md = session.query(MetaTable).get(source_url)
    etl = PlenarioETL(md.as_dict())
    etl.update()
    return 'yay'
