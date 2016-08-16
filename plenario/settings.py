import boto.ec2
import boto.utils
from os import environ
get = environ.get

SECRET_KEY = get('SECRET_KEY', 'abcdefghijklmnop')
PLENARIO_SENTRY_URL = get('PLENARIO_SENTRY_URL', None)
CELERY_SENTRY_URL = get('CELERY_SENTRY_URL', None)
DATA_DIR = '/tmp'

DB_USER = get('DB_USER', '')
DB_PASSWORD = get('DB_PASSWORD', '')
DB_HOST = get('DB_HOST', '')
DB_PORT = get('DB_PORT', 0)
DB_NAME = get('DB_NAME', '')

RS_USER = get('RS_USER', '')
RS_PASSWORD = get('RS_PASSWORD', '')
RS_HOST = get('RS_HOST', '')
RS_PORT = get('RS_PORT', 0)
RS_NAME = get('RS_NAME', '')

DATABASE_CONN = 'postgresql://{}:{}@{}:{}/{}'.\
    format(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)
REDSHIFT_CONN = 'redshift+psycopg2://{}:{}@{}:{}/{}'.\
    format(RS_USER, RS_PASSWORD, RS_HOST, RS_PORT, RS_NAME)

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

AWS_ACCESS_KEY = get('AWS_ACCESS_KEY_ID', '')
AWS_SECRET_KEY = get('AWS_SECRET_ACCESS_KEY', '')
S3_BUCKET = get('S3_BUCKET', '')
AWS_REGION_NAME = get('AWS_REGION_NAME', 'us-east-1')

# Email address for notifying site administrators
# Expect comma-delimited list of emails.
email_list = get('ADMIN_EMAILS')
if email_list:
    ADMIN_EMAILS = email_list.split(',')
else:
    ADMIN_EMAILS = []

# For emailing users. ('MAIL_USERNAME' is an email address.)
MAIL_SERVER = get('MAIL_SERVER', 'smtp.gmail.com')
MAIL_PORT = 587
MAIL_USE_TLS = True
MAIL_DISPLAY_NAME = 'Plenar.io Team'
MAIL_USERNAME = get('MAIL_USERNAME', '')
MAIL_PASSWORD = get('MAIL_PASSWORD', '')

# Toggle maintenence mode
MAINTENANCE = False

# SQS Jobs Queue
JOBS_QUEUE = get('JOBS_QUEUE', 'plenario-test-queue')

# Get Instance ID and Autoscaling Group Name

try:
    instance_metadata = boto.utils.get_instance_metadata(timeout=2, num_retries=2)
    INSTANCE_ID = instance_metadata["instance-id"]
except KeyError:
    print "Could not get INSTANCE_ID"
    INSTANCE_ID = ""

try:
    AUTOSCALING_GROUP = ""
    ec2 = boto.ec2.connect_to_region(
        AWS_REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    reservations = ec2.get_all_instances()
    for res in reservations:
        for inst in res.instances:
            if inst.id == INSTANCE_ID:
                AUTOSCALING_GROUP = inst.tags["aws:autoscaling:groupName"]
                break
        if AUTOSCALING_GROUP:
            break
except boto.exception.EC2ResponseError:
    print "Could not get AUTOSCALING_GROUP"
