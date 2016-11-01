import logging
import os
import subprocess

from scripts.process_running import is_process_running

logging.basicConfig()

if os.environ.get('WORKER'):
    # Guard against worker being run multiple times on one machine.
    if not is_process_running("worker.py"):
        subprocess.Popen(["python", "worker.py"])
        print("Spawned worker process.")
    from plenario.update import create_worker
    application = create_worker()
else:
    from plenario import create_app
    application = create_app()

if __name__ == "__main__":
    should_run_debug = os.environ.get('DEBUG') is not None
    application.run(debug=should_run_debug, host="0.0.0.0")
