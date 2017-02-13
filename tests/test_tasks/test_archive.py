import boto3
import csv
import os
import tarfile
import unittest

from datetime import timedelta
from dateutil.parser import parse
from random import randrange
from sqlalchemy import MetaData
from sqlalchemy import Table, Column, String, DateTime, Float
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import sessionmaker

from plenario.database import redshift_engine as engine
from plenario.settings import S3_BUCKET
from plenario.tasks import archive


metadata = MetaData()

temperature = Table(
    'temperature',
    metadata,
    Column('node_id', String),
    Column('datetime', DateTime),
    Column('meta_id', Float),
    Column('sensor', String),
    Column('temperature', Float))


class TestArchive(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        cls.session = sessionmaker(bind=engine)()

        try:
            temperature.create(engine)
        except ProgrammingError:
            pass

        dt = parse('2017-01-01 00:00:00')
        for i in range(0, 100):
            insert = temperature.insert().values(
                node_id='0',
                datetime=dt + timedelta(days=i),
                meta_id='0',
                sensor='0',
                temperature=randrange(0, 100))
            cls.session.execute(insert)
        cls.session.commit()

    def test_archive(self):

        archive('2017-01')

        s3 = boto3.client('s3')
        with open('test.tar.gz', 'wb') as file:
            s3.download_fileobj(S3_BUCKET, '2017-1/0.tar.gz', file)

        tar = tarfile.open('test.tar.gz')
        tar.extractall()

        with open('0--temperature--2017-01-01--2017-02-01.csv') as file:
            count = 0
            reader = csv.reader(file)
            for line in reader:
                count += 1
            # 31 records plus the header!
            self.assertEquals(count, 32)

        tar.close()

        os.remove('test.tar.gz')
        os.remove('0--temperature--2017-01-01--2017-02-01.csv')

    @classmethod
    def tearDownClass(cls):

        temperature.drop(bind=engine)
        cls.session.close()
