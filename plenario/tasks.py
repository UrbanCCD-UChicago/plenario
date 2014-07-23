import os
from urlparse import urlparse
from plenario.utils.crime_helpers import update_dat_crimes, \
    update_master, chg_crime, update_crime_current_flag, \
    update_master_current_flag, cleanup_temp_tables
from plenario import make_celery
from plenario.models import MetaTable
from plenario.database import task_session as session, Base, task_engine
from plenario.utils.helpers import initialize_table, insert_raw_data
# from raven.handlers.logging import SentryHandler
# from raven.conf import setup_logging
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy import Table

# handler = SentryHandler(os.environ['CELERY_SENTRY_URL'])
# setup_logging(handler)

celery_app = make_celery()

@celery_app.task
def update_crime(fpath=None):
    raw_crime(fpath=fpath)
    dedupe_crime()
    src_crime()
    new = new_crime()
    if new is not None:
        update_dat_crimes()
        update_master()
        chg_crime()
        update_crime_current_flag()
        update_master_current_flag()
    cleanup_temp_tables()
    return None

@celery_app.task
def add_dataset(source_url):
    md = session.query(MetaTable).get(source_url)
    fpath = initialize_table(source_url)
    insert_raw = insert_raw_data(fpath, md.as_dict())
    return 'yay'
