"""Remove dat_ from point table names

Revision ID: 36a266e175ac
Revises: 
Create Date: 2015-12-30 10:35:34.438501

"""

# revision identifiers, used by Alembic.
revision = '36a266e175ac'
down_revision = None
branch_labels = None
depends_on = None

import os, sys
from alembic import op
import sqlalchemy as sa


# Add plenario's root directory to the working path.
pwd = os.path.dirname(os.path.realpath(__file__))
plenario_path = os.path.join(pwd, '../../..')
sys.path.append(str(plenario_path))

from plenario.alembic.version_helpers import dataset_names


def upgrade():
    for dset_name in dataset_names(op):
        old_name = 'dat_' + dset_name
        op.rename_table(old_name, dset_name)


