import os
from urlparse import urlparse
from plenario.celery_app import celery_app
from plenario.models import MetaTable
from plenario.database import task_session as session
from plenario.utils.etl import PlenarioETL
from raven.handlers.logging import SentryHandler
from raven.conf import setup_logging
from plenario.settings import CELERY_SENTRY_URL

if CELERY_SENTRY_URL:
    handler = SentryHandler(CELERY_SENTRY_URL)
    setup_logging(handler)

@celery_app.task
def add_dataset(four_by_four):
    md = session.query(MetaTable).get(four_by_four)
    etl = PlenarioETL(md.as_dict())
    etl.add()
    return 'yay'

@celery_app.task
def daily_update():
    md = session.query(MetaTable)\
        .filter(MetaTable.update_freq == 'daily').all()
    for m in md:
        update_dataset.delay(m.four_by_four)
    return 'yay'

@celery_app.task
def hourly_update():
    md = session.query(MetaTable)\
        .filter(MetaTable.update_freq == 'hourly').all()
    for m in md:
        update_dataset.delay(m.source_url)
    return 'yay'

@celery_app.task
def update_dataset(four_by_four):
    
    md = session.query(MetaTable).get(four_by_four)
    etl = PlenarioETL(md.as_dict())
    etl.update()
    return 'yay'
