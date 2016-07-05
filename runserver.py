import logging
import os, subprocess

from plenario import create_app
from plenario.update import create_worker
logging.basicConfig()

if os.environ.get('WORKER'):
    print "spawning workers"
    for i in range(4):
	    subprocess.Popen(["python", "workerloop.py", str(i)])
    print "workers have spawned."
    application = create_worker()
else:
    application = create_app()
    

if __name__ == "__main__":
    should_run_debug = os.environ.get('DEBUG') is not None
    application.run(debug=should_run_debug)
