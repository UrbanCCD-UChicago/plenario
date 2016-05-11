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
    application.run(debug=True)
