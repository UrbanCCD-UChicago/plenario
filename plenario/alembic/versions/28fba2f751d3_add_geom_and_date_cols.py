"""Add geom and date cols

Revision ID: 28fba2f751d3
Revises: 39614db888c
Create Date: 2015-12-30 10:37:41.191173

"""

# revision identifiers, used by Alembic.
revision = '28fba2f751d3'
down_revision = '39614db888c'
branch_labels = None
depends_on = None

import os, sys
from alembic import op
import sqlalchemy as sa
from geoalchemy2 import Geometry
from sqlalchemy.dialects.postgres import TIMESTAMP

# Add plenario's root directory to the working path.
pwd = os.path.dirname(os.path.realpath(__file__))
plenario_path = os.path.join(pwd, '../../..')
sys.path.append(str(plenario_path))

from plenario.alembic.version_helpers import dataset_names


def upgrade():
    for name in dataset_names(op):
        op.add_column(name, sa.Column('geom', Geometry('POINT', srid=4326)))
        op.add_column(name, sa.Column('point_date', TIMESTAMP,))


