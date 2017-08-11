import os
from tests.base import PlenarioBaseTest
from plenario.database import postgres_base, psql
from plenario.etl.point import PlenarioETL
from plenario.models import MetaTable


class TestPointEtl(PlenarioBaseTest):

    @classmethod
    def setUpClass(cls):
        postgres_base.metadata.create_all()
        psql('plenario/dbscripts/point_from_location.sql')

    @classmethod
    def tearDownClass(cls):
        postgres_base.metadata.drop_all()

    def test_ingest_of_non_ascii_column_headers_and_data(self):
        metatable = MetaTable(
            human_name='データ',
            observed_date='date',
            location='場所',
            url='1'
        )

        path = os.path.join('tests/fixtures/', 'データ.csv')
        PlenarioETL(metatable, source_path=path).add()
        data = self.session.execute('select * from deta')
        self.assertEqual(len(data.fetchall()), 10)
