"""Move geom and date cols to point tables; rm SCD cols

Revision ID: 4164acd6b66a
Revises: 2b7aa5b368df
Create Date: 2016-01-03 19:09:45.618722

"""

# revision identifiers, used by Alembic.
revision = '4164acd6b66a'
down_revision = '2b7aa5b368df'
branch_labels = None
depends_on = None

import os, sys
from sqlalchemy import select, func

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
        original_cols = [c for c in pt.c if c.name not in {name + '_row_id',
                                                           'start_date',
                                                           'end_date',
                                                           'current_flag',
                                                           'dup_ver'}]

        # Create new table with populated geom and date columns
        cols = original_cols + [func.ST_AsEWKB(mt.c.location_geom).label('geom'),
                                mt.c.obs_date.label('point_date')]

        sel = select(cols)\
              .where(mt.c.dataset_row_id == pt.c[name + '_row_id'])


        sel_str = str(sel) + " AND dat_master.dataset_name='{}'".format(name)
        ins_str = "CREATE TABLE tmp AS " + sel_str

        session.execute(ins_str)

        # Drop old table and give new one its name
        rename = 'DROP TABLE "{name}"; ALTER TABLE tmp RENAME TO "{name}";'.format(name=name)
        session.execute(rename)
        session.commit()

    session.close()

