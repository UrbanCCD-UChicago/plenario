import csv
import os
import tarfile
import unittest

from datetime import timedelta
from dateutil.parser import parse as date_parse
from sqlalchemy import create_engine, MetaData
from sqlalchemy import Table, Column, String, DateTime, Float
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import sessionmaker

from plenario.tasks import archive


uri = 'postgresql://postgres:password@localhost:5432'
engine = create_engine(uri)
connection = engine.connect()
metadata = MetaData()


temperature = Table(
    'temperature',
    metadata,
    Column('node_id', String),
    Column('datetime', DateTime),
    Column('meta_id', Float),
    Column('sensor', String))


class TestArchive(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        connection.execute('commit')
        try:
            connection.execute('drop database plenario_test')
        except ProgrammingError:
            pass
        connection.execute('commit')
        connection.execute('create database plenario_test')

        cls.engine = create_engine(uri + '/plenario_test')
        cls.session = sessionmaker(bind=cls.engine)()
        temperature.create(cls.engine)

        time = date_parse('2016-01-01 00:00:00')
        for i in range(0, 100):
            insert = temperature.insert().values(
                node_id='0',
                datetime=time + timedelta(days=i),
                meta_id='0',
                sensor='0')
            cls.session.execute(insert)
        cls.session.commit()

    def test_archive_january(self):

        name = archive('./t.tar.gz', 'temperature', '2016-01-01', '2016-01-31')

        tar = tarfile.open('t.tar.gz')
        tar.extractall()
        tar.close()

        filename = name.split('/', 1)[-1]
        file = open(filename)
        reader = csv.reader(file)

        line_num = 0
        for row in reader:
            line_num += 1

        self.assertEqual(line_num, 31)

        file.close()
        os.remove(filename)
        os.rmdir('tmp')
        os.remove('t.tar.gz')

    def test_archive_february(self):

        name = archive('./t.tar.gz', 'temperature', '2016-02-01', '2016-02-29')

        tar = tarfile.open('t.tar.gz')
        tar.extractall()
        tar.close()

        filename = name.split('/', 1)[-1]
        file = open(filename)
        reader = csv.reader(file)

        line_num = 0
        for row in reader:
            line_num += 1

        self.assertEqual(line_num, 29)

        file.close()
        os.remove(filename)
        os.rmdir('tmp')
        os.remove('t.tar.gz')
