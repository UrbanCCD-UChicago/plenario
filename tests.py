import os
import json
import unittest
from test_fixtures.test_models import Base, Master, Crime, BusinessLicense
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import csv

os.environ['WOPR_CONN'] = 'postgresql://wopr:@localhost:5432/wopr_test'
CONN = os.environ['WOPR_CONN']

from app import app

def make_rows(fname):
    with open(fname, 'rb') as f:
        reader = csv.DictReader(f)
        for row in reader:
            for k, v in row.items():
                if v == '':
                    row[k] = None
            yield row

class WoprTest(unittest.TestCase):
    # TODO: Genuinely figure out how to make and destroy the DB on the fly
    # Need to do this now:
    # createdb -U wopr -O wopr wopr_test
    # psql -U postgres -d wopr_test -c "create extension postgis;"
    # psql -U postgres -d wopr_test -c "grant all on geometry_columns to "wopr";"
    # psql -U postgres -d wopr_test -c "grant all on spatial_ref_sys to "wopr";"
    # and then this to destroy:
    # dropdb -U wopr wopr_test
    @classmethod
    def setUpClass(self):
        engine = create_engine(CONN)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        self.session = Session()
        for row in make_rows('test_fixtures/master.csv'):
            self.session.add(Master(**row))
        for row in make_rows('test_fixtures/crime.csv'):
            self.session.add(Crime(**row))
        for row in make_rows('test_fixtures/business.csv'):
            self.session.add(BusinessLicense(**row))
        self.session.commit()
        self.app = app.test_client()

    def test_fields(self):
        fields = self.app.get('/api/fields/chicago_crimes_all/')
        observed_names = [f['field_name'] for f in json.loads(fields.data)]
        expected_names = Crime.__table__.columns.keys()
        self.assertEqual(observed_names, expected_names)

if __name__ == "__main__":
    unittest.main()
