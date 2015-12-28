"""Populate date and geom cols

Revision ID: 4b1e44c83b12
Revises: 28fba2f751d3
Create Date: 2015-12-30 10:45:33.296922

"""

# revision identifiers, used by Alembic.
revision = '4b1e44c83b12'
down_revision = '28fba2f751d3'
branch_labels = None
depends_on = None

import os, sys
from sqlalchemy import select


# Add plenario's root directory to the working path.
pwd = os.path.dirname(os.path.realpath(__file__))
plenario_path = os.path.join(pwd, '../../..')
sys.path.append(str(plenario_path))

from plenario.database import session
from plenario.models import MetaTable, MasterTable


def dataset_names():
    return [row.dataset_name for row in session.query(MetaTable.dataset_name).
                                                filter_by(approved_status='true').all()]


def upgrade():
    mt = MasterTable.__table__
    for name in dataset_names():
        print 'updating ' + name
        pt = MetaTable.get_by_dataset_name(name).point_table

        # Select geoms and dates from matching records in master table
        sel = select([mt.c.location_geom.label('geom'),
                      mt.c.observed_date.label('date')])\
              .where(mt.c.master_row_id == pt.c[name + '_row_id'])\
              .limit(1)
        upd = pt.update().values(geom=sel.geom, point_date=sel.date)

        session.execute(upd)
        session.commit()

    session.close()
