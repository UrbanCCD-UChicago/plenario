PLENARIO_SENTRY_URL = None
CELERY_SENTRY_URL = None
DATABASE_CONN = 'postgresql://willengler:@localhost:5432/plenario_test'
DATA_DIR = '/Users/willengler/Sandbox/test-plenario'

# See: https://pythonhosted.org/Flask-Cache/#configuring-flask-cache
# for config options
CACHE_CONFIG = {
    'CACHE_TYPE': 'simple',
}

AWS_ACCESS_KEY = ''
AWS_SECRET_KEY = ''
S3_BUCKET = ''

# Optional dict with attributes for a default web admin
DEFAULT_USER = {
    'name': 'plenario_user',
    'email': 'youremail@example.com',
    'password': 'your password'
}

# Email address for notifying site administrators
ADMIN_EMAIL = ''

# For emailing users. ('MAIL_USERNAME' is an email address.)
MAIL_SERVER = 'smtp.gmail.com'
MAIL_PORT = 587
MAIL_USE_TLS = True
MAIL_DISPLAY_NAME = 'Plenar.io Team'
MAIL_USERNAME = ''
MAIL_PASSWORD = ''


CENSUS_BLOCKS = {
    'dataset_name': 'census_blocks',
    'business_key': 'geoid',
    'source_url': 'http://www2.census.gov/geo/tiger/TIGER2010/TABBLOCK/2010/tl_2010_17031_tabblock10.zip'
}

# Toggle maintenence mode
MAINTENANCE = False
