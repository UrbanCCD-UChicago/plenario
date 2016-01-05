"""Dedupe

Revision ID: 2b7aa5b368df
Revises: 4fe83cd32f79
Create Date: 2016-01-03 17:22:13.779043

"""

# revision identifiers, used by Alembic.
revision = '2b7aa5b368df'
down_revision = '4fe83cd32f79'
branch_labels = None
depends_on = None

import os, sys
from alembic import op


# Add plenario's root directory to the working path.
pwd = os.path.dirname(os.path.realpath(__file__))
plenario_path = os.path.join(pwd, '../../..')
sys.path.append(str(plenario_path))

from plenario.alembic.version_helpers import dataset_names
from plenario.database import session
from plenario.models import MetaTable


def upgrade():
    for dset_name in dataset_names(op):
        meta = MetaTable.get_by_dataset_name(dset_name)
        pt = meta.point_table

        # Deduplicate business key by only taking dup_ver > 1
        dedupe = pt.delete().where(pt.c.dup_ver > 1)
        session.execute(dedupe)

        # Remove columns where business key is null
        denull = pt.delete().where(meta.id_col() == None)
        session.execute(denull)

    session.close()
