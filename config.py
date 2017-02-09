import os


class Default:

    BASE_URI = 'postgresql://postgres:password@localhost:5432'
    DB_NAME = 'plenario_test'
    POSTGRES_URI = BASE_URI + '/plenario_dev'
    REDSHIFT_URI = BASE_URI + '/plenario_dev_rs'

    USERNAME = 'plenario'
    EMAIL = 'plenario@admin.com'
    PASSWORD = 'changemeplz'


class DevConfig(Default):

    POSTGRES_URI = os.environ.get('DEV_POSTGRES_URI') or Default.POSTGRES_URI
    REDSHIFT_URI = os.environ.get('DEV_REDSHIFT_URI') or Default.REDSHIFT_URI
    S3_BUCKET = os.environ.get('DEV_S3_BUCKET') or 'plenario-dev'


class TestConfig(Default):

    POSTGRES_URI = os.environ.get('TEST_POSTGRES_URI') or Default.POSTGRES_URI
    REDSHIFT_URI = os.environ.get('TEST_REDSHIFT_URI') or Default.REDSHIFT_URI
    S3_BUCKET = os.environ.get('TEST_S3_BUCKET') or 'plenario-test'


class ProdConfig:

    POSTGRES_URI = os.environ.get('POSTGRES_URI')
    REDSHIFT_URI = os.environ.get('REDSHIFT_URI')
    S3_BUCKET = os.environ.get('S3_BUCKET')


configs = {
    'dev': DevConfig,
    'test': TestConfig,
    'prod': ProdConfig
}

Config = configs[os.environ.get('CONFIG') or 'dev']


Config.CELERY_RESULT_BACKEND = 'db+' + Config.POSTGRES_URI
