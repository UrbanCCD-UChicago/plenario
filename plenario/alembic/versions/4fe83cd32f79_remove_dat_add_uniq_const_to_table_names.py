"""Remove dat, add uniq const to table names

Revision ID: 4fe83cd32f79
Revises: 
Create Date: 2016-01-03 17:11:35.048990

"""

# revision identifiers, used by Alembic.
revision = '4fe83cd32f79'
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


def upgrade():
    for dset_name in dataset_names(op):
        old_name = 'dat_' + dset_name
        op.rename_table(old_name, dset_name)

    op.create_unique_constraint('unique_dataset_name', 'meta_master', ['dataset_name'])