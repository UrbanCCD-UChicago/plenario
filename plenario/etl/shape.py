import zipfile

from sqlalchemy.exc import ProgrammingError
from plenario.database import postgres_engine, postgres_session
# from plenario.etl.common import ETLFile, add_unique_hash
from plenario.utils.shapefile import import_shapefile


class ShapeETL:

    def __init__(self, meta, source_path=None):
        self.source_path = source_path
        self.table_name = meta.dataset_name
        self.source_url = meta.source_url
        self.meta = meta

    def add(self):
        staging_name = 'staging_{}'.format(self.table_name)

        # with ETLFile(self.source_path, self.source_url, interpret_as='bytes') as file_helper:
        #     handle = open(file_helper.handle.name, "rb")
        #     with zipfile.ZipFile(handle) as shapefile_zip:
        #         import_shapefile(shapefile_zip, staging_name)
        #         add_unique_hash(staging_name)

        try:
            postgres_engine.execute('drop table {}'.format(self.table_name))
        except ProgrammingError:
            pass

        rename_table = 'alter table {} rename to {}'
        rename_table = rename_table.format(staging_name, self.table_name)
        postgres_engine.execute(rename_table)
        
        self.meta.update_after_ingest()
        postgres_session.commit()

    def update(self):
        self.add()
