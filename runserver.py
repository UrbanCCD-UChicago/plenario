import logging
import os

from plenario import create_app
from plenario.update import create_worker
logging.basicConfig()

if os.environ.get('WORKER'):
    application = create_worker()
else:
    application = create_app()

if __name__ == "__main__":
    should_run_debug = os.environ.get('DEBUG') is not None
    application.run(debug=should_run_debug)
