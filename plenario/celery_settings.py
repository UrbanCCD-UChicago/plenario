from plenario.settings import DB_USER, DB_HOST, DB_PASSWORD, DB_PORT, DB_NAME, \
                              REDIS_HOST

BROKER_URL = 'redis://{}:6379/0'.format(REDIS_HOST)
CELERY_RESULT_BACKEND = 'db+postgresql://{}:{}@{}:{}/{}'.format(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)

CELERY_IMPORTS = ('plenario.tasks',)
CELERY_TIMEZONE = 'America/Chicago'
CELERYD_HIJACK_ROOT_LOGGER = False
CELERY_TASK_RESULT_EXPIRES = None
