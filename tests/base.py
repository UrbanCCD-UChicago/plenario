from flask_testing import TestCase

from plenario.database import psql
from plenario.server import create_app, db


class PlenarioTestCase(TestCase):

    def setUp(self):
        psql('./plenario/dbscripts/sensor_tree.sql')
        psql('./plenario/dbscripts/point_from_location.sql')
        db.create_all()

    def tearDown(self):
        db.metadata.drop_all(bind=db.engine)
        db.engine.execute('drop schema if exists public cascade')
        db.engine.execute('create schema public')
        db.engine.execute('create extension postgis')
        db.session.commit()

    def create_app(self):
        return create_app()
