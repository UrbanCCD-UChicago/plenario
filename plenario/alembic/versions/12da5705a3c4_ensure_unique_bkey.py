"""ensure unique bkey

Revision ID: 12da5705a3c4
Revises: 2a7f30b6b20d
Create Date: 2016-01-04 18:35:55.395932

"""

# revision identifiers, used by Alembic.
revision = '12da5705a3c4'
down_revision = '2a7f30b6b20d'
branch_labels = None
depends_on = None

import os, sys
from alembic import op


# Add plenario's root directory to the working path.
pwd = os.path.dirname(os.path.realpath(__file__))
plenario_path = os.path.join(pwd, '../../..')
sys.path.append(str(plenario_path))

from plenario.alembic.version_helpers import dataset_names
from plenario.models import MetaTable
from plenario.database import session


def upgrade():
    for name in dataset_names(op):

        # Make business key the primary key
        meta = MetaTable.get_by_dataset_name(name)
        bkey_col_name = meta.business_key
        session.close()

        # I don't know how duplicate bkeys snuck in, but they did.


        dedupe = """
            CREATE SEQUENCE tmp_seq;
            ALTER TABLE "{table}" ADD COLUMN seq INTEGER NOT NULL default nextval('tmp_seq');

            DELETE FROM "{table}" USING "{table}" alias
              WHERE "{table}"."{bkey}" = alias."{bkey}" AND "{table}".seq < alias.seq;
              ALTER TABLE "{table}" DROP COLUMN seq;
              DROP SEQUENCE tmp_seq;
              """.format(table=name, bkey=bkey_col_name)

        session.execute(dedupe)

        make_pkey = """
           ALTER TABLE "{table_name}" ADD PRIMARY KEY ({col_name})""".\
            format(table_name=name, col_name=bkey_col_name)

        session.execute(make_pkey)
        session.commit()
        print 'Added pkey to ' + name