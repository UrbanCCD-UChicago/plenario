"""Update geom columns with geoms from master

Revision ID: 42eee6f4f98c
Revises: 4984c00162d5
Create Date: 2015-11-19 13:33:50.425487

"""

# revision identifiers, used by Alembic.
revision = '42eee6f4f98c'
down_revision = '4984c00162d5'
branch_labels = None
depends_on = None

import os
import sys

pwd = os.path.dirname(os.path.realpath(__file__))
plenario_path = os.path.join(pwd, '../../..')
sys.path.append(str(plenario_path))

from plenario.alembic.version_helpers import dataset_names
from plenario.database import app_engine
from plenario.models import MetaTable, MasterTable
import sqlalchemy as sa

def upgrade():
    mt = MasterTable.__table__
    for name in dataset_names():
        pt = MetaTable.get_by_dataset_name(name).point_table
        sel = sa.select([mt.c.location_geom])\
                .where(mt.c.master_row_id == pt.c.point_id)\
                .limit(1)
        upd = pt.update().values(geom=sel)
        app_engine.execute(upd)


def downgrade():
    pass
