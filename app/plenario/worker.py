from datetime import datetime
from flask import Flask
from logging import getLogger

import plenario.tasks as tasks

logger = getLogger(__name__)


def create_worker():

    app = Flask(__name__)
    app.config.from_object('plenario.settings')
    app.url_map.strict_slashes = False

    @app.route('/update/weather', methods=['POST'])
    def weather():
        return tasks.update_weather.delay().id

    @app.route('/update/often', methods=['POST'])
    def metar():
        return tasks.update_metar.delay().id

    @app.route('/update/<frequency>', methods=['POST'])
    def update(frequency):
        return tasks.frequency_update.delay(frequency).id

    @app.route('/archive', methods=['POST'])
    def archive():
        return tasks.archive.delay(datetime.now()).id

    @app.route('/health', methods=['GET', 'POST'])
    def check_health():
        return tasks.health.delay().id

    logger.info('Running in worker mode')
    return app
