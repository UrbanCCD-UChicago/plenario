#! /usr/bin/env python

import signal
import subprocess
import warnings

from flask.exthook import ExtDeprecationWarning
from flask_script import Manager
from os import getenv

from plenario import create_app as server
from plenario.settings import DB_HOST, DB_NAME, DB_PORT, DB_USER, DB_PASSWORD
from plenario.settings import RS_HOST, RS_NAME, RS_PORT, RS_USER, RS_PASSWORD
from plenario.settings import DATABASE_CONN, REDSHIFT_CONN
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
    """Start up plenario server"""

    application.run(host="0.0.0.0", port=5000)


@manager.command
def worker():
    """Start up celery worker"""

    celery_commands = ["celery", "-A", "plenario.tasks", "worker", "-l", "INFO"]
    wait(subprocess.Popen(celery_commands))


@manager.command
def monitor():
    """Start up flower task monitor"""

    flower_commands = ["flower", "-A", "plenario.tasks", "--persistent"]
    wait(subprocess.Popen(flower_commands))


@manager.command
def pg():
    """Psql into postgres"""

    print("[plenario] Connecting to %s" % DATABASE_CONN)
    wait(subprocess.Popen([
        "env", "PGPASSWORD=%s" % DB_PASSWORD,
        "psql",
        "-d", DB_NAME,
        "-h", DB_HOST,
        "-U", DB_USER,
        "-p", str(DB_PORT),
    ]))


@manager.command
def rs():
    """Psql into redshift"""

    print("[plenario] Connecting to %s" % REDSHIFT_CONN)
    wait(subprocess.Popen([
        "env", "PGPASSWORD=%s" % RS_PASSWORD,
        "psql",
        "-d", RS_NAME,
        "-h", RS_HOST,
        "-U", RS_USER,
        "-p", str(RS_PORT),
    ]))


@manager.command
def test():
    """Run nosetests"""

    nose_commands = ["nosetests", "-s", "tests", "-vv"]
    wait(subprocess.Popen(nose_commands))


def wait(process):
    """Waits on a process and passes along sigterm"""

    try:
        signal.pause()
    except (KeyboardInterrupt, SystemExit):
        process.terminate()
        process.wait()


if __name__ == "__main__":
    manager.run()
