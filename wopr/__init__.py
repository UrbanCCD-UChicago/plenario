import os
from flask import Flask
from celery.schedules import crontab
from wopr.database import session as db_session
from wopr.api import api

CELERYBEAT_SCHEDULE = {
    'update_crime_every_day': {
        'task': 'wopr.tasks.update_crime',
        'schedule': crontab(minute=0, hour=8),
    }
}

def create_app():
    app = Flask(__name__)
    app.url_map.strict_slashes = False
    app.register_blueprint(api)
    app.config['CELERY_IMPORTS'] = ('wopr.tasks',)
    app.config['CELERYBEAT_SCHEDULE'] = CELERYBEAT_SCHEDULE
    app.config['CELERY_TIMEZONE'] = 'America/Chicago'
    @app.teardown_appcontext
    def shutdown_session(exception=None):
        db_session.remove()
    return app
