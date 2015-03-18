import os
from urlparse import urlparse
from plenario.celery_app import celery_app
from plenario.models import MetaTable, MasterTable
from plenario.database import task_session as session, task_engine as engine, \
    Base
from plenario.utils.etl import PlenarioETL
from plenario.utils.weather import WeatherETL
from raven.handlers.logging import SentryHandler
from raven.conf import setup_logging
from plenario.settings import CELERY_SENTRY_URL
from sqlalchemy import Table
from sqlalchemy.exc import NoSuchTableError, InternalError
from datetime import datetime

if CELERY_SENTRY_URL:
    handler = SentryHandler(CELERY_SENTRY_URL)
    setup_logging(handler)

@celery_app.task(bind=True)
def delete_dataset(self, source_url_hash):
    md = session.query(MetaTable).get(source_url_hash)
    try:
        dat_table = Table('dat_%s' % md.dataset_name, Base.metadata, 
            autoload=True, autoload_with=engine, keep_existing=True)
        dat_table.drop(engine, checkfirst=True)
    except NoSuchTableError:
        pass
    master_table = MasterTable.__table__
    delete = master_table.delete()\
        .where(master_table.c.dataset_name == md.dataset_name)
    conn = engine.contextual_connect()
    try:
        conn.execute(delete)
        session.delete(md)
        session.commit()
    except InternalError, e:
        raise delete_dataset.retry(exc=e)
    conn.close()
    return 'Deleted {0} ({1})'.format(md.human_name, md.source_url_hash)

@celery_app.task(bind=True)
def add_dataset(self, source_url_hash, s3_path=None, data_types=None):
    md = session.query(MetaTable).get(source_url_hash)
    if md.result_ids:
        ids = md.result_ids
        ids.append(self.request.id)
    else:
        ids = [self.request.id]
    with engine.begin() as c:
        c.execute(MetaTable.__table__.update()\
            .where(MetaTable.source_url_hash == source_url_hash)\
            .values(result_ids=ids))
    etl = PlenarioETL(md.as_dict(), data_types=data_types)
    etl.add(s3_path=s3_path)
    return 'Finished adding {0} ({1})'.format(md.human_name, md.source_url_hash)

@celery_app.task
def frequency_update(frequency):
    # hourly, daily, weekly, monthly, yearly
    md = session.query(MetaTable)\
        .filter(MetaTable.update_freq == frequency).all()
    for m in md:
        update_dataset.delay(m.source_url_hash)
    return '%s update complete' % frequency

@celery_app.task(bind=True)
def update_dataset(self, source_url_hash, s3_path=None):
    md = session.query(MetaTable).get(source_url_hash)
    if md.result_ids:
        ids = md.result_ids
        ids.append(self.request.id)
    else:
        ids = [self.request.id]
    with engine.begin() as c:
        c.execute(MetaTable.__table__.update()\
            .where(MetaTable.source_url_hash == source_url_hash)\
            .values(result_ids=ids))
    etl = PlenarioETL(md.as_dict())
    etl.update(s3_path=s3_path)
    return 'Finished updating {0} ({1})'.format(md.human_name, md.source_url_hash)

@celery_app.task
def update_metar():
    stations = 'blah'
    print "hello update_metar()"
    

@celery_app.task
def update_weather():
    month, year = datetime.now().month, datetime.now().year
    stations = ['94846', '14855', '04807', '14819', '94866', '04831', '04838']
    w = WeatherETL()
    w.initialize_month(year, month, weather_stations_list=stations)
    return 'Added weather for %s %s' % (month, year)
