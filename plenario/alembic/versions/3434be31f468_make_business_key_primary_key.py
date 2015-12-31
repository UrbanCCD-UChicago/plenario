"""Make business_key primary key

Revision ID: 3434be31f468
Revises: 4b1e44c83b12
Create Date: 2015-12-30 10:49:03.980468

"""

# revision identifiers, used by Alembic.
revision = '3434be31f468'
down_revision = '4b1e44c83b12'
branch_labels = None
depends_on = None

import os, sys
from alembic import op


# Add plenario's root directory to the working path.
pwd = os.path.dirname(os.path.realpath(__file__))
plenario_path = os.path.join(pwd, '../../..')
sys.path.append(str(plenario_path))

from plenario.alembic.version_helpers import dataset_names
from plenario.database import session
from plenario.models import MetaTable


def upgrade():
    for dset_name in dataset_names(op):
        meta = MetaTable.get_by_dataset_name(dset_name)
        pt = meta.point_table
        bkey_col_name = meta.business_key

        # Deduplicate business key by only taking dup_ver > 1
        dedupe = pt.delete().where(pt.c.dup_ver > 1)
        session.execute(dedupe)

        # Remove columns where business key is null
        denull = pt.delete().where(meta.id_col() == None)
        session.execute(denull)

        # Make business key the primary key
        make_pkey = """
           ALTER TABLE "{table_name}" ADD PRIMARY KEY ({col_name})""".\
            format(table_name=dset_name, col_name=bkey_col_name)

        session.execute(make_pkey)
        session.commit()
        session.close()
        print 'Added pkey to ' + dset_name
