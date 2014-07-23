import os
from flask import Flask
from celery import Task, Celery
from celery.schedules import crontab
from raven.contrib.flask import Sentry
from plenario.database import session as db_session
from plenario.api import api
from plenario.views import views
from urllib import quote_plus

BROKER_URL = 'redis://localhost:6379/0'

CELERYBEAT_SCHEDULE = {
    'update_crime_every_day': {
        'task': 'plenario.tasks.update_crime',
        'schedule': crontab(minute=0, hour=8),
    }
}

sentry = Sentry(dsn=os.environ['WOPR_SENTRY_URL'])

def create_app():
    app = Flask(__name__)
    app.url_map.strict_slashes = False
    app.register_blueprint(api)
    app.register_blueprint(views)
    sentry.init_app(app)
    @app.teardown_appcontext
    def shutdown_session(exception=None):
        db_session.remove()
    return app

def make_celery(app=None):
    app = app or create_app()
    celery_app = Celery(app.import_name, broker=BROKER_URL)
    celery_app.conf['CELERY_IMPORTS'] = ('plenario.tasks',)
    celery_app.conf['CELERYBEAT_SCHEDULE'] = CELERYBEAT_SCHEDULE
    celery_app.conf['CELERY_TIMEZONE'] = 'America/Chicago'
    celery_app.conf['CELERYD_HIJACK_ROOT_LOGGER'] = False
    celery_app.conf['CELERY_IGNORE_RESULT'] = True
    TaskBase = celery_app.Task
    class ContextTask(TaskBase):
        abstract = True
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)
    celery_app.Task  = ContextTask
    return celery_app
