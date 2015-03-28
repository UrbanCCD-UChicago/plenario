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
    print "update_metar()"
    celery_metar_illinois_area_wbans = [u'14855', u'54808', u'14834', u'04838', u'04876', u'03887', u'04871', u'04873', u'04831', u'04879', u'04996', u'14880', u'04899', u'94892', u'94891', u'04890', u'54831', u'94870', u'04894', u'94854', u'14842', u'93822', u'04807', u'04808', u'54811', u'94822', u'94846', u'04868', u'04845', u'04896', u'04867', u'04866', u'04889', u'14816', u'04862', u'94866', u'04880', u'14819']
    ohare_mdw= ['94846', '14819']
    w = WeatherETL()
    w.metar_initialize_current(weather_stations_list = celery_metar_illinois_area_wbans)
    #w.metar_initialize_current(weather_stations_list = ohare_mdw)
    return 'Added current metars'

@celery_app.task
def update_weather():
    month, year = datetime.now().month, datetime.now().year
    stations = ['94846', '14855', '04807', '14819', '94866', '04831', '04838']
    w = WeatherETL()
    w.initialize_month(year, month, weather_stations_list=stations)
    return 'Added weather for %s %s' % (month, year)
