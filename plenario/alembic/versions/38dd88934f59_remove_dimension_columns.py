"""Remove dimension columns

Revision ID: 38dd88934f59
Revises: 3434be31f468
Create Date: 2015-12-30 11:12:48.860001

"""

# revision identifiers, used by Alembic.
revision = '38dd88934f59'
down_revision = '3434be31f468'
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
    cols_to_drop = ['dup_ver', 'start_date', 'end_date', 'current_flag']
    for name in dataset_names(op):
        for col in cols_to_drop:
            op.drop_column(name, col)
