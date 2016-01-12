"""Add indexes on date and geom

Revision ID: 14ffcba87e11
Revises: 12da5705a3c4
Create Date: 2016-01-04 21:13:50.865160

"""

# revision identifiers, used by Alembic.
revision = '14ffcba87e11'
down_revision = '12da5705a3c4'
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
    for name in dataset_names(op):
        # Index names can only be 63 chars long
        # So make the part of the index name determined by the dataset be max 54 chars
        trunc = name[:54]

        print 'indexing ' + name

        op.create_index('ix_{}_date'.format(trunc), name, ['point_date'])
        op.create_index('ix_{}_geom'.format(trunc), name, ['geom'])