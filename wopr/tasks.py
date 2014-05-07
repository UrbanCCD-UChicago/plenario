import os
from datetime import datetime, timedelta
from wopr import make_celery
from wopr.database import session
from wopr.helpers import raw_crime, dedupe_crime, src_crime, new_crime, \
    update_dat_crimes, update_master, update_crime_current_flag, \
    update_master_current_flag

celery_app = make_celery()

@celery_app.task
def update_crime():
    raw_crime()
    dedupe_crime()
    src_crime()
    new_crime()
    update_dat_crimes()
    update_master()
    chg_crime()
    update_crime_current_flag()
    update_master_current_flag()
    return None
