"""Make geom col PostGIS type; rm dup_ver;

Revision ID: 2a7f30b6b20d
Revises: 4164acd6b66a
Create Date: 2016-01-04 08:31:00.643872

"""

# revision identifiers, used by Alembic.
revision = '2a7f30b6b20d'
down_revision = '4164acd6b66a'
branch_labels = None
depends_on = None

import os, sys
from alembic import op
import sqlalchemy as sa


# Add plenario's root directory to the working path.
pwd = os.path.dirname(os.path.realpath(__file__))
plenario_path = os.path.join(pwd, '../../..')
sys.path.append(str(plenario_path))


from plenario.alembic.version_helpers import dataset_names


def upgrade():
    for name in dataset_names(op):
        print 'Updating geom type of ' + name

        alter = """ALTER TABLE "{}"
        ALTER COLUMN geom TYPE geometry(Point,4326);""".format(name)
        op.execute(alter)
