from datetime import datetime, timedelta

from raven.conf import setup_logging
from raven.handlers.logging import SentryHandler
from sqlalchemy.exc import NoSuchTableError, InternalError

from plenario.celery_app import celery_app
from plenario.database import session as session, app_engine as engine
from plenario.etl.shape import ShapeETL
from plenario.models import MetaTable, ShapeMetadata
from plenario.settings import CELERY_SENTRY_URL
from plenario.etl.point import PlenarioETL
from plenario.utils.weather import WeatherETL

if CELERY_SENTRY_URL:
    handler = SentryHandler(CELERY_SENTRY_URL)
    setup_logging(handler)

@celery_app.task(bind=True)
def delete_dataset(self, source_url_hash):
    md = session.query(MetaTable).get(source_url_hash)
    try:
        dat_table = md.point_table
        dat_table.drop(engine, checkfirst=True)
    except NoSuchTableError:
        # Move on so we can get rid of the metadata
        pass
    session.delete(md)
    try:
        session.commit()
    except InternalError, e:
        raise delete_dataset.retry(exc=e)
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
    md.contributed_data_types = data_types
    etl = PlenarioETL(md)
    etl.add(s3_path=s3_path)
    return 'Finished adding {0} ({1})'.format(md.human_name, md.source_url_hash)

@celery_app.task(bind=True)
def add_shape(self, table_name):

    # Associate the dataset with this celery task so we can check on the task's status
    meta = session.query(ShapeMetadata).get(table_name)
    meta.celery_task_id = self.request.id
    session.commit()

    # Ingest the shapefile
    ShapeETL(meta=meta).import_shapefile()
    return 'Finished adding shape dataset {} from {}.'.format(meta.dataset_name, meta.source_url)

@celery_app.task(bind=True)
def delete_shape(self, table_name):
    shape_meta = session.query(ShapeMetadata).get(table_name)
    shape_meta.remove_table(caller_session=session)
    session.commit()
    return 'Removed {}'.format(table_name)

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


@celery_app.task()
def hello_world():
    """
    Used in init_db for its side effect.
    Just running a task will create the celery_taskmeta tables in the database/
    """
    print "Hello from celery!"

@celery_app.task
def update_weather():
    # This should do the current month AND the previous month, just in case.

    lastMonth_dt = datetime.now() - timedelta(days=1)
    lastMonth = lastMonth_dt.month
    lastYear = lastMonth_dt.year

    month, year = datetime.now().month, datetime.now().year
    #stations = ['12921', '94740', '14855', '03981', '93894', '94746', '93816', '93904', '03887', '14752', '93810', '53981', '04953', '14776', '14777', '53983', '54723', '14770', '14771', '94765', '94761', '93986', '14778', '03968', '53952', '53950', '94892', '94891', '54757', '13802', '23293', '54734', '54735', '54733', '04789', '64775', '04787', '64776', '04783', '54738', '54739', '93823', '93822', '14740', '23272', '53802', '54811', '13922', '03985', '14742', '03972', '04868', '14747', '53822', '93227', '93817', '93228', '14748', '04867', '04866', '03879', '23289', '53891', '04862', '53944', '53947', '64705', '53942', '64707', '23203', '53935', '54704', '63853', '94982', '14834', '64761', '04751', '04876', '04871', '04873', '04739', '04879', '93232', '93231', '14758', '13902', '14754', '23240', '94741', '14757', '14750', '94979', '94745', '14753', '53939', '12910', '03868', '14923', '03902', '53933', '54831', '94870', '13904', '14775', '64758', '13911', '04996', '64753', '12911', '63840', '63841', '64757', '64756', '13909', '04903', '23250', '23257', '04807', '23254', '04724', '04808', '04726', '23258', '04720', '03999', '13984', '94908', '13986', '23285', '04889', '14931', '54789', '54788', '64706', '94868', '14733', '94866', '54782', '54781', '03948', '54787', '54786', '54785', '03919', '14719', '54768', '54760', '04925', '93997', '54767', '94721', '94723', '94725', '03969', '53999', '04725', '23244', '94728', '12961', '23907', '14739', '53913', '54792', '54793', '14732', '93211', '04950', '14736', '14737', '14734', '14735', '04899', '53938', '04890', '03928', '04896', '04894', '94854', '53997', '94822', '54778', '54779', '13966', '13967', '04949', '14786', '54770', '94959', '54777', '23239', '94733', '93943', '04947', '14703', '14702', '12979', '04742', '23230', '04741', '14990', '23234', '53986', '12971', '94846', '23237', '03932', '04930', '53909', '53897', '54808', '13919', '14816', '03838', '14819', '03933', '63814', '63817', '63810', '53886', '13958', '14790', '14792', '54756', '14794', '93942', '54790', '04838', '93978', '14712', '13961', '14714', '14715', '14717', '63878', '04831', '94705', '94704', '94702', '54773', '13999', '14880', '93947', '03958', '94789', '93984', '93985', '53977', '53976', '64774', '53979', '93989', '54742', '53150', '23211', '13940', '14937', '13945', '04921', '14842', '53887', '04880', '53889', '54771', '04781', '94794', '94790', '14707', '54780', '14761', '14760', '14763', '14762', '04845', '14768', '54746', '53964', '53965', '54743', '63901', '54740', '13809', '03950', '53969', '03957', '03954', '94737']
    stations = ['94846', '14855', '04807', '14819', '94866', '04831', '04838']
    w = WeatherETL()
    if (lastMonth != month):
        w.initialize_month(lastYear, lastMonth, weather_stations_list=stations)
    w.initialize_month(year, month, weather_stations_list=stations)

    # Given that this was the most recent month, year, call this function,
    # which will figure out the most recent hourly weather observation and
    # delete all metars before that datetime.
    w.clear_metars(year, month, weather_stations_list=stations)
    return 'Added weather for %s %s' % (month, year)
