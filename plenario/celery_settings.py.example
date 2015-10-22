from celery.schedules import crontab
from plenario.settings import DB_USER, DB_HOST, DB_PASSWORD, DB_PORT, DB_NAME

BROKER_URL = 'redis://localhost:6379/0'
CELERY_RESULT_BACKEND = 'db+postgresql://{}:{}@{}:{}/{}'.format(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)

CELERYBEAT_SCHEDULE = {
    'monthly_update': {
        'task': 'plenario.tasks.frequency_update',
        'args': ('monthly',),
        'schedule': crontab(0, 0, 0, day_of_month=1),
    },
    'weekly_update': {
        'task': 'plenario.tasks.frequency_update',
        'args': ('weekly',),
        'schedule': crontab(minute=0, hour=0, day_of_week='sunday'),
    },
    'daily_update': {
        'task': 'plenario.tasks.frequency_update',
        'args': ('daily',),
        'schedule': crontab(minute=0, hour=8),
    },
    'hourly_update': {
        'task': 'plenario.tasks.frequency_update',
        'args': ('hourly',),
        'schedule': crontab(minute=0)
    }
}

CELERY_IMPORTS = ('plenario.tasks',)
CELERYBEAT_SCHEDULE = CELERYBEAT_SCHEDULE
CELERY_TIMEZONE = 'America/Chicago'
CELERYD_HIJACK_ROOT_LOGGER = False
CELERY_TASK_RESULT_EXPIRES = None
