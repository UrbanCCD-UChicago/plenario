from .base import PlenarioBaseTest, load_fixture
from plenario.database import postgres_base, psql


class TestDatasetsApi(PlenarioBaseTest):

    @classmethod
    def setUpClass(cls):
        postgres_base.metadata.create_all()
        psql('plenario/dbscripts/point_from_location.sql')

    @classmethod
    def tearDownClass(cls):
        postgres_base.metadata.drop_all()

    def test_dataset_name__in_query_argument(self):
        load_fixture('clinics.csv', 'clinics1', '1', date='date', location='location')
        load_fixture('clinics.csv', 'clinics2', '2', date='date', location='location')
        load_fixture('clinics.csv', 'clinics3', '3', date='date', location='location')
        response = self.client.get('/v1/api/datasets?dataset_name__in=clinics1,clinics2')
        self.assertEqual(response.json['meta']['total'], 2)
        self.assertEqual(len(response.json['objects']), 2)
