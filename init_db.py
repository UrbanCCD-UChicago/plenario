from plenario.database import session, app_engine, Base
import plenario.models
import plenario.settings
from sqlalchemy.exc import IntegrityError
import datetime
from plenario.utils.weather import WeatherETL, WeatherStationsETL
from plenario.utils.shape_etl import ShapeETL

from plenario.tasks import hello_world


def init_db():
    init_master_meta_user()
    init_weather()
    init_census()
    init_celery()


def init_master_meta_user():
    print 'creating master, meta and user tables'
    Base.metadata.create_all(bind=app_engine)
    if plenario.settings.DEFAULT_USER:
        print 'creating default user %s' % plenario.settings.DEFAULT_USER['name']
        user = plenario.models.User(**plenario.settings.DEFAULT_USER)
        session.add(user)
        try:
            session.commit()
        except IntegrityError:
            pass


def init_weather():
    print 'initializing NOAA weather stations'
    s = WeatherStationsETL()
    s.initialize()

    print 'initializing NOAA daily and hourly weather observations for %s/%s' % (datetime.datetime.now().month, datetime.datetime.now().year)
    print 'this will take a few minutes ...'
    e = WeatherETL()
    e.initialize_month(datetime.datetime.now().year, datetime.datetime.now().month)


def init_census():
    print 'initializing and populating US Census blocks'
    print 'this will *also* take a few minutes ...'
    census_settings = plenario.settings.CENSUS_BLOCKS

    # Only try to cache to AWS if we've specified a key
    save_to_s3 = (plenario.settings.AWS_ACCESS_KEY != '')

    census_meta = plenario.models.ShapeMetadata.add(source_url=census_settings['source_url'],
                                                      human_name=census_settings['human_name'],
                                                      caller_session=session)
    session.commit()
    ShapeETL(meta=census_meta, save_to_s3=save_to_s3).import_shapefile()


def init_celery():
    hello_world.delay()

if __name__ == "__main__":
    init_db()
