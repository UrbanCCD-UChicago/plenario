import subprocess

from os import getenv

from plenario import create_app as server
from plenario.worker import create_worker as worker


apps = {
    "server": server,
    "worker": worker
}

application = apps["worker"]() if getenv("WORKER", None) else apps["server"]()


if getenv("WORKER", False):
    subprocess.Popen(["celery", "-A", "plenario.tasks", "worker", "-l", "INFO"])
