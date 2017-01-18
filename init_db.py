import sqlalchemy.exc
import subprocess

from argparse import ArgumentParser
from sqlalchemy.engine.base import Engine

from plenario.database import session, app_engine, Base
from plenario.settings import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT
from plenario.settings import DEFAULT_USER


sensor_meta_table_names = (
    "sensor__network_metadata",
    "sensor__node_metadata",
    "sensor__features_of_interest",
    "sensor__sensors",
    "sensor__sensor_to_node",
    "sensor__feature_to_network"
)


# todo: remove "createdb plenario_test" step from the readme
def create_database(engine: Engine, database_name: str) -> None:
    """Setup a database (schema) in postgresql. If the database creation fails,
    say why and move on."""

    try:
        connection = engine.connect()
        connection.execute("commit")
        connection.execute("create database %s" % database_name)
        connection.close()
    except sqlalchemy.exc.ProgrammingError as exc:
        print(exc)


# todo: remove "create extension postgis" step from the readme
def create_postgis_extension(engine: Engine) -> None:
    """Setup the postgis extension in postgresql. If the extension creation
    fails, say why and move on."""

    try:
        connection = engine.connect()
        connection.execute("create extension postgis")
        connection.close()
    except sqlalchemy.exc.ProgrammingError as exc:
        print(exc)


def create_tables(tables):
    """Helper to initialize the tables from the Base.metadata store

    :param tables: (iterable) of string table names"""

    for table in Base.metadata.sorted_tables:
        if str(table) in tables:
            print("CREATE TABLE: {}".format(table))
            try:
                table.create(bind=app_engine)
            except sqlalchemy.exc.ProgrammingError as exc:
                print(exc)
                print("ALREADY EXISTS: {}".format(table))


def delete_tables(tables):
    """The opposite of create_tables.
    
    :param tables: (iterable) of string table names"""

    for table in tables:
        try:
            app_engine.execute("DROP TABLE {} CASCADE".format(table))
            print("DROP TABLE {}".format(table))
        except sqlalchemy.exc.ProgrammingError:
            print("ALREADY DOESN'T EXIST: {}".format(table))
        

def init_db(args):
    print("")
    print("================")
    print("Plenario INIT DB")
    print("================")
    if not any(vars(args).values()):
        # No specific arguments specified. Run it all!
        init_meta()
        create_tables(sensor_meta_table_names)
        add_functions()
        init_user()
        init_worker_meta()
        init_weather()
    else:
        if args.meta:
            init_meta()
        if args.users:
            init_user()
        if args.weather:
            init_weather()
        if args.workers:
            init_worker_meta()
        if args.functions:
            add_functions()
        if args.sensors:
            create_tables(sensor_meta_table_names)
        if args.delete_sensors:
            delete_tables(sensor_meta_table_names)


def init_meta():
    create_tables(("meta_master", "meta_shape"))


def init_user():
    create_tables(("plenario_user",))

    if DEFAULT_USER['name']:
        from plenario.models import User
        if session.query(User).count() > 0:
            print('Users already exist. Skipping this step.')
            return

        print('Creating default user %s' % DEFAULT_USER['name'])
        user = User(**DEFAULT_USER)
        session.add(user)
        try:
            session.commit()
        except Exception as e:
            session.rollback()
            print("Problem while creating default user: ", e)
    else:
        print('No default user specified. Skipping this step.')


def init_weather():
    pass
    # print('initializing NOAA weather stations')
    # s = WeatherStationsETL()
    # s.initialize()
    #
    # print('initializing NOAA daily and hourly weather observations for %s/%s' %
    #       (datetime.datetime.now().month, datetime.datetime.now().year))
    # print('this will take a few minutes ...')
    # e = WeatherETL()
    # try:
    #     e.initialize_month(
    #         datetime.datetime.now().year,
    #         datetime.datetime.now().month
    #     )
    # except Exception as e:
    #     session.rollback()
    #     raise e


def init_worker_meta():
    create_tables(('plenario_workers', 'plenario_datadump', 'etl_task'))


def add_functions():

    def add_function(script_path):
        args = 'PGPASSWORD=' + DB_PASSWORD
        args += ' psql '
        args += ' -h ' + DB_HOST
        args += ' -U ' + DB_USER
        args += ' -d ' + DB_NAME
        args += ' -p ' + str(DB_PORT)
        args += ' -f ' + script_path
        subprocess.check_output(args, shell=True)

    add_function("./plenario/dbscripts/audit_trigger.sql")
    add_function("./plenario/dbscripts/point_from_location.sql")
    add_function("./plenario/dbscripts/sensors_trigger.sql")


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
    parser.add_argument('-f', '--functions', action='store_true',
                        help='Add plenario-specific functions to database.')
    parser.add_argument("-s", "--sensors", action="store_true",
                        help="Initialize tables for working with AOT data.")
    parser.add_argument("-k", "--workers", action="store_true",
                        help="Initialze tables for plenario's worker system.")
    parser.add_argument("--delete-sensors", action="store_true")
    return parser


if __name__ == "__main__":

    create_database(app_engine, "plenario_test")
    create_postgis_extension(app_engine)

    argparser = build_arg_parser()
    arguments = argparser.parse_args()
    init_db(arguments)
