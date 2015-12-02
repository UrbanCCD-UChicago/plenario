from plenario.database import session, app_engine, Base
import plenario.models
import plenario.settings
from sqlalchemy.exc import IntegrityError
import datetime
from argparse import ArgumentParser
from plenario.utils.weather import WeatherETL, WeatherStationsETL
from plenario.utils.shape_etl import ShapeETL

from plenario.tasks import hello_world


def init_db(args={}):
    if args.everything:
        init_master_meta_user()
        init_weather()
        init_census()
        init_celery()
    else:
        if args.tables:
            init_master_meta_user()
        if args.weather:
            init_weather()
        if args.census:
            init_census()
        if args.celery:
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

def build_arg_parser():
    '''Creates an argument parser for this script. This is helpful in the event
    that a user needs to only run a portion of the setup script.
    '''
    description = 'Set up your development environment with this script. It \
    creates tables, initializes NOAA weather station data and US Census block \
    data.'
    parser = ArgumentParser(description=description)
    parser.add_argument('-t', '--tables', dest='tables', help='Set up the \
            master, meta and user tables')
    parser.add_argument('-w', '--weather', dest='weather', help='Set up NOAA \
            weather station data. This includes the daily and hourly weather \
            observations.')
    parser.add_argument('-c', '--census', dest='census', help='Set up and \
            populate US Census blocks.')
    parser.add_argument('-cl', '--celery', dest='celery', help='Say hello \
            world from Celery')
    parser.add_argument('-e', '--everything', dest='everything', help='Run \
            everything in the script.', default=True)
    return parser

if __name__ == "__main__":
    parser = build_arg_parser()
    arguments = parser.parse_args()
    init_db(arguments)
