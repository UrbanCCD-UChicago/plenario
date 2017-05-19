#! /usr/bin/env python

import os
import signal
import subprocess
import warnings

from flask.exthook import ExtDeprecationWarning
from flask_script import Manager
from kombu.exceptions import OperationalError
from os import getenv
from sqlalchemy.exc import IntegrityError, ProgrammingError, InvalidRequestError

from plenario import create_app as server
from plenario.settings import Config
from plenario.settings import DEFAULT_USER
from plenario.worker import create_worker as worker


# Ignore warnings stating that libraries we depend on use deprecated flask code
warnings.filterwarnings("ignore", category=ExtDeprecationWarning)
# Ignore warnings stating that our forms do not address all model fields
warnings.filterwarnings("ignore", "Fields missing from ruleset", UserWarning)


apps = {
    "server": server,
    "worker": worker
}

application = apps["worker"]() if getenv("WORKER", None) else apps["server"]()
manager = Manager(application)


@manager.command
def runserver():
    """Start up plenario server."""

    application.run(host="0.0.0.0", port=5000, debug=os.environ.get('DEBUG'))


@manager.command
def worker():
    """Start up celery worker."""

    celery_commands = ["celery", "-A", "plenario.tasks", "worker", "-l", "INFO"]
    wait(subprocess.Popen(celery_commands))


@manager.command
def monitor():
    """Start up flower task monitor."""

    flower_commands = ["flower", "-A", "plenario.tasks", "--persistent"]
    wait(subprocess.Popen(flower_commands))


@manager.command
def pg():
    """Psql into postgres."""

    print("[plenario] Connecting to %s" % Config.DATABASE_CONN)
    wait(subprocess.Popen(["psql", Config.DATABASE_CONN]))


@manager.command
def rs():
    """Psql into redshift."""

    print("[plenario] Connecting to %s" % Config.REDSHIFT_CONN)
    wait(subprocess.Popen(["psql", Config.REDSHIFT_CONN]))


@manager.command
def test():
    """Run nosetests."""

    nose_commands = ["env", "PLENARIO_CONFIG=test"]
    nose_commands += ["nosetests", "-s", "tests", "-v"]
    nose_commands += ["--with-coverage", "--cover-package=plenario"]
    wait(subprocess.Popen(nose_commands))


@manager.command
def config():
    """Set up environment variables for plenario."""

    pass


@manager.command
def init():
    """Initialize the database."""

    from plenario.database import create_database
    from sqlalchemy import create_engine

    base_uri = Config.DATABASE_CONN.rsplit('/', 1)[0]
    base_engine = create_engine(base_uri)

    try:
        create_database(base_engine, Config.DB_NAME)
    except ProgrammingError:
        print('[plenario] It already exists!')

    from plenario.database import create_extension
    from plenario.database import postgres_engine, postgres_base
    from plenario.utils.weather import WeatherETL, WeatherStationsETL

    try:
        create_extension(postgres_engine, 'postgis')
    except ProgrammingError:
        print('[plenario] It already exists!')

    print('[plenario] Creating metadata tables')
    try:
        postgres_base.metadata.create_all()
    except ProgrammingError:
        print('[plenario] It already exists!')

    print('[plenario] Creating weather tables')
    try:
        WeatherStationsETL().make_station_table()
    except InvalidRequestError:
        print('[plenario] It already exists!')
    WeatherETL().make_tables()

    from plenario.database import psql

    # Set up custom functions, triggers and views in postgres
    psql("./plenario/dbscripts/audit_trigger.sql")
    psql("./plenario/dbscripts/point_from_location.sql")

    # Set up the default user if we are running in anything but production
    if os.environ.get('CONFIG') != 'prod':
        from plenario.database import postgres_session
        from plenario.models.User import User

        print('[plenario] Create default user')
        user = User(**DEFAULT_USER)

        try:
            postgres_session.add(user)
            postgres_session.commit()
        except IntegrityError:
            print('[plenario] Already exists!')
            postgres_session.rollback()

    from plenario.tasks import health

    # This will get celery to set up its meta tables
    try:
        health.delay()
    except OperationalError:
        print('[plenario] Redis is not running!')


@manager.command
def uninstall():
    """Drop the plenario databases."""

    from sqlalchemy import create_engine
    from plenario.database import drop_database

    base_uri = Config.DATABASE_CONN.rsplit('/', 1)[0]
    base_engine = create_engine(base_uri)
    try:
        drop_database(base_engine, 'plenario_test')
    except ProgrammingError:
        pass

    base_uri = Config.REDSHIFT_CONN.rsplit('/', 1)[0]
    base_engine = create_engine(base_uri)
    try:
        drop_database(base_engine, 'plenario_test')
    except ProgrammingError:
        pass


def wait(process):
    """Waits on a process and passes along sigterm."""

    try:
        signal.pause()
    except (KeyboardInterrupt, SystemExit):
        process.terminate()
        process.wait()


if __name__ == "__main__":
    manager.run()
