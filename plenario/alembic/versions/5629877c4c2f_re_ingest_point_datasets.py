"""Re-ingest point datasets

Revision ID: 5629877c4c2f
Revises: 
Create Date: 2016-01-20 09:37:40.780808

"""

# revision identifiers, used by Alembic.
revision = '5629877c4c2f'
down_revision = None
branch_labels = None
depends_on = None

import os, sys
from alembic import op

# Add plenario's root directory to the working path.
pwd = os.path.dirname(os.path.realpath(__file__))
plenario_path = os.path.join(pwd, '../../..')
sys.path.append(str(plenario_path))


from plenario.alembic.version_helpers import dataset_names
from plenario.database import session, app_engine as engine
from plenario.models import MetaTable


def upgrade():
    for dset_name in dataset_names(op):
        # Generate columns to hash on
        print dset_name
        pt = MetaTable.get_by_dataset_name(dset_name).point_table
        session.close()

        names_to_hash = [name for name in pt.c.keys()
                         if name not in {'hash', 'point_date', 'geom'}]
        quoted_names = ['"{}"'.format(name) for name in names_to_hash]
        concatted_names = ','.join(quoted_names)

        add_hash = '''
        DROP TABLE IF EXISTS temp;
        CREATE TABLE temp AS
          SELECT DISTINCT {col_names},
                 md5(CAST(({col_names})AS text))
                    AS hash, geom, point_date FROM "{table_name}";
        DROP TABLE "{table_name}";
        ALTER TABLE temp RENAME TO "{table_name}";
        ALTER TABLE "{table_name}" ADD PRIMARY KEY (hash);
        '''.format(table_name=dset_name, col_names=concatted_names)

        engine.execute(add_hash)

