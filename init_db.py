import plenario.models
import plenario.settings
from plenario.database import session, app_engine, Base
from plenario.tasks import hello_world
from plenario.settings import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DEFAULT_USER
from plenario.utils.weather import WeatherETL, WeatherStationsETL
from argparse import ArgumentParser
import os
import datetime
import subprocess


def init_db(args):
    if not any(vars(args).values()):
        # No specific arguments specified. Run it all!
        init_master_meta_user()
        init_celery()
        add_functions()
    else:
        if args.meta:
            init_meta()
        if args.users:
            init_user()
        if args.weather:
            init_weather()
        if args.celery:
            init_celery()
        if args.functions:
            add_functions()


def init_master_meta_user():
    print 'creating master, meta and user tables'
    init_meta()
    init_user()


def init_meta():
    #Reset concept of a metadata table
    non_meta_tables = [table for table in Base.metadata.sorted_tables
                       if table.name not in
                       {'meta_master', 'meta_shape', 'plenario_user'}]
    for t in non_meta_tables:
        Base.metadata.remove(t)
    
    #Create databases (if they don't exist)
    Base.metadata.create_all(bind=app_engine)

def init_user():
    if DEFAULT_USER['name']:
        print 'Creating default user %s' % DEFAULT_USER['name']
        if session.query(plenario.models.User).count() > 0:
            print 'Users already exist. Skipping this step.'
            return
        user = plenario.models.User(**DEFAULT_USER)
        session.add(user)
        try:
            session.commit()
        except Exception as e:
            session.rollback()
            print "Problem while creating default user: ", e
    else:
        print 'No default user specified. Skipping this step.'


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


def add_functions():
    def add_function(script_path):
        args = 'PGPASSWORD='+DB_PASSWORD+' psql -h '+DB_HOST+' -U '+DB_USER+' -d '+DB_NAME+' -f '+script_path
        subprocess.check_output(args, shell=True)
        #Using shell=True otherwise it seems that aws doesn't have the proper paths.

    add_function("./plenario/dbscripts/audit_trigger.sql")
    add_function("./plenario/dbscripts/point_from_location.sql")

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
    parser.add_argument('-m', '--meta', action="store_true",
                        help="Set up the metadata registries needed to"
                             " ingest point and shape datasets.")
    parser.add_argument('-u', '--users', action="store_true",
                        help='Set up the a default\
                              user to access the admin panel.')
    parser.add_argument('-w', '--weather', action="store_true",
                        help='Set up NOAA weather station data.\
                              This includes the daily and hourly weather \
                              observations.')
    parser.add_argument('-cl', '--celery', action="store_true",
                        help='Say hello world from Celery')
    parser.add_argument('-f', '--functions', action='store_true',
                        help='Add plenario-specific functions to database.')
    return parser

if __name__ == "__main__":
    parser = build_arg_parser()
    arguments = parser.parse_args()
    init_db(arguments)
