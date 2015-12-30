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
        original_cols = [c for c in pt.c if c.name not in {'geom', 'point_date'}]

        # Create new table with populated geom and date columns
        sel = select(original_cols + [
                      mt.c.location_geom.label('geom'),
                      mt.c.obs_date.label('point_date')])\
              .where(mt.c.master_row_id == pt.c[name + '_row_id'])

        sel_str = str(sel)
        ins_str = "CREATE TABLE tmp AS " + sel_str

        session.execute(ins_str)

        # Drop old table and give new one its name
        rename = 'DROP TABLE "{name}"; ALTER TABLE tmp RENAME TO "{name}";'.format(name=name)
        session.execute(rename)
        session.commit()

    session.close()
