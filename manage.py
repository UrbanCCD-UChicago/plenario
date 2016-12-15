#! /usr/bin/env python

import subprocess

from flask_script import Manager
from os import getenv

from plenario import create_app as server
from plenario.settings import DB_HOST, DB_NAME, DB_PORT, DB_USER, DB_PASSWORD
from plenario.settings import RS_HOST, RS_NAME, RS_PORT, RS_USER, RS_PASSWORD
from plenario.settings import DATABASE_CONN, REDSHIFT_CONN
from plenario.worker import create_worker as worker


apps = {
    "server": server,
    "worker": worker
}

application = apps["worker"]() if getenv("WORKER", None) else apps["server"]()
manager = Manager(application)


@manager.command
def server():
    application.run(host="0.0.0.0", port="5000")


@manager.command
def worker():
    subprocess.call(["celery", "-A", "plenario.tasks", "worker", "-l", "INFO"])


@manager.command
def monitor():
    subprocess.call(["flower", "-A", "plenario.tasks", "persistent=True"])


@manager.command
def pg():
    print("[plenario] Connecting to %s" % DATABASE_CONN)
    subprocess.call([
        "env", "PGPASSWORD=%s" % DB_PASSWORD,
        "psql",
        "-d", DB_NAME,
        "-h", DB_HOST,
        "-U", DB_USER,
        "-p", str(DB_PORT),
    ])


@manager.command
def rs():
    print("[plenario] Connecting to %s" % REDSHIFT_CONN)
    subprocess.call([
        "env", "PGPASSWORD=%s" % RS_PASSWORD,
        "psql",
        "-d", RS_NAME,
        "-h", RS_HOST,
        "-U", RS_USER,
        "-p", str(RS_PORT),
    ])


if __name__ == "__main__":
    manager.run()
