from os import environ


get = environ.get


SECRET_KEY = get('SECRET_KEY', 'abcdefghijklmnop')

PLENARIO_SENTRY_URL = get('PLENARIO_SENTRY_URL', None)

DATA_DIR = '/tmp'

# Travis CI relies on the default values to build correctly,
# just keep in mind that if you push changes to the default
# values, you need to make sure to adjust for these changes
# in the travis.yml
DB_USER = get('POSTGRES_USER', 'postgres')
DB_PASSWORD = get('POSTGRES_PASSWORD', 'password')
DB_HOST = get('POSTGRES_HOST', 'localhost')
DB_PORT = get('POSTGRES_PORT', 5432)
DB_NAME = get('POSTGRES_DB', 'plenario_test')

RS_USER = get('REDSHIFT_USER', 'postgres')
RS_PASSWORD = get('REDSHIFT_PASSWORD', 'password')
RS_HOST = get('REDSHIFT_HOST', 'localhost')
RS_PORT = get('REDSHIFT_PORT', 5432)
RS_NAME = get('REDSHIFT_NAME', 'plenario_test')

DATABASE_CONN = 'postgresql://{}:{}@{}:{}/{}'.format(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)
REDSHIFT_CONN = 'postgresql://{}:{}@{}:{}/{}'.format(RS_USER, RS_PASSWORD, RS_HOST, RS_PORT, RS_NAME)

# Use this cache for data that can be refreshed
REDIS_HOST = get('REDIS_HOST', 'localhost')

# See: https://pythonhosted.org/Flask-Cache/#configuring-flask-cache
# for config options
CACHE_CONFIG = {
    'CACHE_TYPE': 'redis',
    'CACHE_REDIS_HOST': REDIS_HOST,
    'CACHE_KEY_PREFIX': get('CACHE_KEY_PREFIX', 'plenario_app')
}

# Load a default admin
DEFAULT_USER = {
    'name': get('DEFAULT_USER_NAME', 'Plenario Admin'),
    'email': get('DEFAULT_USER_EMAIL', 'plenario@email.com'),
    'password': get('DEFAULT_USER_PASSWORD', 'changemeplz')
}

# Amazon Web Services
AWS_ACCESS_KEY = get('AWS_ACCESS_KEY', '')
AWS_SECRET_KEY = get('AWS_SECRET_KEY', '')
AWS_REGION_NAME = get('AWS_REGION_NAME', 'us-east-1')
S3_BUCKET = get('S3_BUCKET', '')

# Email address for notifying site administrators
# Expect comma-delimited list of emails.
_admin_emails = get('ADMIN_EMAILS')
if _admin_emails:
    ADMIN_EMAILS = _admin_emails.split(',')
else:
    ADMIN_EMAILS = []

# For emailing users. ('MAIL_USERNAME' is an email address.)
MAIL_SERVER = get('MAIL_SERVER', 'smtp.gmail.com')
MAIL_PORT = 587
MAIL_USE_TLS = True
MAIL_DISPLAY_NAME = 'Plenar.io Team'
MAIL_USERNAME = get('MAIL_USERNAME', '')
MAIL_PASSWORD = get('MAIL_PASSWORD', '')

# Toggle maintenance mode
MAINTENANCE = False

# Celery
CELERY_BROKER_URL = get('CELERY_BROKER_URL', 'redis://{}:6379/0'.format(REDIS_HOST))
CELERY_RESULT_BACKEND = get('CELERY_RESULT_BACKEND', 'db+{}'.format(DATABASE_CONN))
FLOWER_URL = get('FLOWER_URL', 'http://localhost:5555')
