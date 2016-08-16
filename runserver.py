import logging
import os
import subprocess

logging.basicConfig()

if os.environ.get('WORKER'):
    from plenario.update import create_worker
    subprocess.Popen(["python", "worker.py"])
    print "Spawned worker process."
    application = create_worker()
else:
    from plenario import create_app
    application = create_app()

if __name__ == "__main__":
    should_run_debug = os.environ.get('DEBUG') is not None
    application.run(debug=should_run_debug)
