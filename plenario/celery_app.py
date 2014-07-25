from celery import Task, Celery
from celery.schedules import crontab

BROKER_URL = 'redis://localhost:6379/0'

# CELERYBEAT_SCHEDULE = {
#     'update_crime_every_day': {
#         'task': 'plenario.tasks.update_crime',
#         'schedule': crontab(minute=0, hour=8),
#     }
# }

celery_app = Celery(__name__, broker=BROKER_URL)
celery_app.conf['CELERY_IMPORTS'] = ('plenario.tasks',)
# celery_app.conf['CELERYBEAT_SCHEDULE'] = CELERYBEAT_SCHEDULE
celery_app.conf['CELERY_TIMEZONE'] = 'America/Chicago'
celery_app.conf['CELERYD_HIJACK_ROOT_LOGGER'] = False
celery_app.conf['CELERY_IGNORE_RESULT'] = True

