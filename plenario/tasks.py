import os
from urlparse import urlparse
from plenario.celery_app import celery_app
from plenario.models import MetaTable, MasterTable
from plenario.database import task_session as session, task_engine as engine, \
    Base
from plenario.utils.etl import PlenarioETL
from raven.handlers.logging import SentryHandler
from raven.conf import setup_logging
from plenario.settings import CELERY_SENTRY_URL
from sqlalchemy import Table

if CELERY_SENTRY_URL:
    handler = SentryHandler(CELERY_SENTRY_URL)
    setup_logging(handler)

@celery_app.task
def delete_dataset(source_url_hash):
    md = session.query(MetaTable).get(source_url_hash)
    dat_table = Table('dat_%s' % md.dataset_name, Base.metadata, 
        autoload=True, autoload_with=engine, keep_existing=True)
    dat_table.drop(engine, checkfirst=True)
    master_table = MasterTable.__table__
    delete = master_table.delete()\
        .where(master_table.c.dataset_name == md.dataset_name)
    conn = engine.contextual_connect()
    conn.execute(delete)
    session.delete(md)
    session.commit()
    return 'Deleted %s' % md.human_name

@celery_app.task
def add_dataset(source_url_hash, s3_path=None, data_types=None):
    md = session.query(MetaTable).get(source_url_hash)
    etl = PlenarioETL(md.as_dict(), data_types=data_types)
    etl.add(s3_path=s3_path)
    return 'Finished adding %s' % md.human_name

@celery_app.task
def monthly_update():
    md = session.query(MetaTable)\
        .filter(MetaTable.update_freq == 'monthly').all()
    for m in md:
        update_dataset.delay(m.source_url_hash)
        print 'Updating %s' % m.human_name
    return 'Weekly update complere'

@celery_app.task
def weekly_update():
    md = session.query(MetaTable)\
        .filter(MetaTable.update_freq == 'weekly').all()
    for m in md:
        update_dataset.delay(m.source_url_hash)
        print 'Updating %s' % m.human_name
    return 'Weekly update complere'

@celery_app.task
def daily_update():
    md = session.query(MetaTable)\
        .filter(MetaTable.update_freq == 'daily').all()
    for m in md:
        update_dataset.delay(m.source_url_hash)
        print 'Updating %s' % m.human_name
    return 'Daily update complete'

@celery_app.task
def hourly_update():
    md = session.query(MetaTable)\
        .filter(MetaTable.update_freq == 'hourly').all()
    for m in md:
        update_dataset.delay(m.source_url_hash)
        print 'Updating %s' % m.human_name
    return 'yay'

@celery_app.task
def update_dataset(source_url_hash, s3_path=None):
    
    md = session.query(MetaTable).get(source_url_hash)
    etl = PlenarioETL(md.as_dict())
    etl.update(s3_path=s3_path)
    return 'Finished updating %s' % md.human_name

@celery_app.task
def update_weather(month, year):
    w = WeatherETL()
    w.initialize_month(month, year)
    return 'Added weather for %s %s' % (month, year)
