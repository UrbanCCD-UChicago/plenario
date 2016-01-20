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
from plenario.database import app_engine as engine
from plenario.models import MetaTable
from plenario.tasks import add_dataset


def upgrade():
    for name in dataset_names(op):
        meta = MetaTable.get_by_dataset_name(name)
        drop = 'DROP TABLE IF EXISTS "{}";'.format(name)
        engine.execute(drop)
        hash = meta.source_url_hash
        add_dataset(hash)
