import os
from datetime import datetime, timedelta
from wopr import make_celery
from wopr.database import session
from wopr.models import MasterTable, MetaTable

celery_app = make_celery()

@celery_app.task
def update_crime():
    return None
