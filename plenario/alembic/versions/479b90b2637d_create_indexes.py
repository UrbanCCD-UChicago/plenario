"""Create indexes

Revision ID: 479b90b2637d
Revises: 42eee6f4f98c
Create Date: 2015-11-30 08:58:15.522826

"""

# revision identifiers, used by Alembic.
revision = '479b90b2637d'
down_revision = '42eee6f4f98c'
branch_labels = None
depends_on = None

import os
import sys

pwd = os.path.dirname(os.path.realpath(__file__))
plenario_path = os.path.join(pwd, '../../..')
sys.path.append(str(plenario_path))

from plenario.alembic.version_helpers import dataset_names
from alembic import op, context
import sqlalchemy as sa


def upgrade():
    for name in dataset_names():
        # Indexes can only be 63 chars long.
        # So make the part of the index name determined by the dataset be max 45 chars
        trunc = name[:45]
        table_name = 'dat_{}'.format(name)

        def create_if_not_exist(ix_name, col):
            if not exists(ix_name):
                op.create_index(ix_name, table_name, [col])

        create_if_not_exist('ix_{}_point_date'.format(trunc), 'point_date')
        create_if_not_exist('ix_{}_point_geom'.format(trunc), 'geom')


def exists(ix_name):
    conn = context.get_context().connection
    sel = sa.text("SELECT to_regclass('{}');".format(ix_name))
    found = conn.execute(sel).first()[0]
    return bool(found)


def downgrade():
    for name in dataset_names():
        trunc = name[:45]

        def drop_if_exist(ix_name):
            if exists(ix_name):
                op.drop_index(ix_name)

        drop_if_exist('ix_{}_point_date'.format(trunc))
        drop_if_exist('ix_{}_point_geom'.format(trunc))
