from os import getenv

from plenario import create_app as server
from plenario.update import create_worker as worker


apps = {
    "server": server,
    "worker": worker
}

application = apps["worker"]() if getenv("WORKER", None) else apps["server"]()