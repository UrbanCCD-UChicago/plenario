#! /usr/bin/env python

import subprocess

from flask_script import Manager
from os import getenv

from plenario import create_app as server
from plenario.update import create_worker as worker


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


if __name__ == "__main__":
    manager.run()
