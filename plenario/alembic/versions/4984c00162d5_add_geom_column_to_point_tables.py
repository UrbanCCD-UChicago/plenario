"""Add geom column to point tables

Revision ID: 4984c00162d5
Revises: 4e960796230e
Create Date: 2015-11-19 13:16:00.256099

"""

# revision identifiers, used by Alembic.
revision = '4984c00162d5'
down_revision = '4e960796230e'
branch_labels = None
depends_on = None

import os
import sys

pwd = os.path.dirname(os.path.realpath(__file__))
plenario_path = os.path.join(pwd, '../../..')
sys.path.append(str(plenario_path))

from alembic import op
from plenario.alembic.version_helpers import dataset_names
import sqlalchemy as sa
from geoalchemy2 import Geometry


def upgrade():
    for name in dataset_names():
        op.add_column('dat_' + name,
                      sa.Column('geom', Geometry('POINT', srid=4326)))


def downgrade():
    for name in dataset_names():
        op.drop_column('dat_' + name, 'geom')
