"""Add indexes on date and geom

Revision ID: d3d4cf596a4
Revises: 38dd88934f59
Create Date: 2015-12-30 11:19:41.686686

"""

# revision identifiers, used by Alembic.
revision = 'd3d4cf596a4'
down_revision = '38dd88934f59'
branch_labels = None
depends_on = None

import os, sys
from alembic import op, context
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

        def create_if_not_exist(ix_name, col):
            if not exists(ix_name):
                op.create_index(ix_name, name, [col])

        create_if_not_exist('ix_{}_date'.format(trunc), 'point_date')
        create_if_not_exist('ix_{}_geom'.format(trunc), 'geom')


def exists(ix_name):
    conn = context.get_context().connection
    sel = sa.text("SELECT to_regclass('{}');".format(ix_name))
    found = conn.execute(sel).first()[0]
    return bool(found)
