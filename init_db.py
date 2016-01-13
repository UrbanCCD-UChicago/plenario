import plenario.models
import plenario.settings
from plenario.database import session, app_engine, Base
from plenario.etl.shape import ShapeETL
from plenario.tasks import hello_world
from plenario.utils.weather import WeatherETL, WeatherStationsETL
from argparse import ArgumentParser

def init_db(args):
    if not any(vars(args).values()):
        # No specific arguments specified. Run it all!
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
        except Exception as e:
            session.rollback()
            raise e


def init_weather():
    print 'initializing NOAA weather stations'
    s = WeatherStationsETL()
    s.initialize()

    print 'initializing NOAA daily and hourly weather observations for %s/%s' % (datetime.datetime.now().month, datetime.datetime.now().year)
    print 'this will take a few minutes ...'
    e = WeatherETL()
    try:
        e.initialize_month(datetime.datetime.now().year, datetime.datetime.now().month)
    except Exception as e:
        session.rollback()
        raise e


def init_census():
    print 'initializing and populating US Census blocks'
    print 'this will *also* take a few minutes ...'
    census_settings = plenario.settings.CENSUS_BLOCKS

    census_meta = plenario.models.ShapeMetadata.add(source_url=census_settings['source_url'],
                                                      human_name=census_settings['human_name'],
                                                      caller_session=session)
    try:
        session.commit()
    except Exception as e:
        session.rollback()
        raise e

    ShapeETL(meta=census_meta, save_to_s3=save_to_s3).import_shapefile()


def init_celery():
    hello_world.delay()


def build_arg_parser():
    """Creates an argument parser for this script. This is helpful in the event
    that a user needs to only run a portion of the setup script.
    """
    description = 'Set up your development environment with this script. It \
    creates tables, initializes NOAA weather station data and US Census block \
    data. If you specify no options, it will populate everything.'
    parser = ArgumentParser(description=description)
    parser.add_argument('-t', '--tables', action="store_true", help='Set up the \
            master, meta and user tables')
    parser.add_argument('-w', '--weather', action="store_true", help='Set up NOAA \
            weather station data. This includes the daily and hourly weather \
            observations.')
    parser.add_argument('-c', '--census', action="store_true", help='Set up and \
            populate US Census blocks.')
    parser.add_argument('-cl', '--celery', action="store_true", help='Say hello \
            world from Celery')
    return parser

if __name__ == "__main__":
    parser = build_arg_parser()
    arguments = parser.parse_args()
    init_db(arguments)
