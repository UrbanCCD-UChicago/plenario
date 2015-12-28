"""Ensure point table names are unique

Revision ID: 39614db888c
Revises: 36a266e175ac
Create Date: 2015-12-30 10:35:54.710095

"""

# revision identifiers, used by Alembic.
revision = '39614db888c'
down_revision = '36a266e175ac'
branch_labels = None
depends_on = None

import os, sys
from alembic import op
import sqlalchemy as sa


# Add plenario's root directory to the working path.
pwd = os.path.dirname(os.path.realpath(__file__))
plenario_path = os.path.join(pwd, '../../..')
sys.path.append(str(plenario_path))


def upgrade():
    op.create_unique_constraint('unique_dataset_name', 'meta_master', ['dataset_name'])

