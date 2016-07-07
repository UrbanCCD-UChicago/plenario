from multiprocessing import Process

from flask import Flask, abort

import plenario.tasks as tasks
from plenario.api.jobs import worker_ready

"""
Task server that runs in AWS Elastic Beanstalk worker environment.
Takes POST requests for cron-scheduled tasks.
Posts most of them to the Celery queue living on Redis,
but also runs METAR updates right away.
"""


def create_worker():
    app = Flask(__name__)
    app.config.from_object('plenario.settings')
    app.url_map.strict_slashes = False

    @app.route('/update/weather', methods=['POST'])
    def weather():
        tasks.update_weather.delay()
        return "Sent off weather task"

    @app.route('/update/<frequency>', methods=['POST'])
    def update(frequency):
        try:
            dispatch[frequency]()
            return "Sent update request"
        except KeyError:
            abort(400)

    @app.route('/health')
    def check_health():
        if worker_ready():
            return "Workers are available."
        else:
            abort(503)

    return app


def often_update():
    # Keep METAR updates out of the queue
    # so that they run right away even when the ETL is chugging through
    # a big backlog of event dataset updates.

    # Run METAR update in new thread
    # so we can return right away to indicate the request was received
    Process(target=tasks.update_metar).start()


def daily_update():
    tasks.update_weather.delay()
    tasks.frequency_update.delay('daily')


def weekly_update():
    tasks.frequency_update.delay('weekly')


def monthly_update():
    tasks.frequency_update.delay('monthly')


def yearly_update():
    tasks.frequency_update.delay('yearly')

dispatch = {
    'often': often_update,
    'daily': daily_update,
    'weekly': weekly_update,
    'monthly': monthly_update,
    'yearly': yearly_update
}
