"""Remove meta_master columns

Revision ID: 1bb31ab6089a
Revises: 
Create Date: 2016-01-26 09:38:30.551522

"""

# revision identifiers, used by Alembic.
revision = '1bb31ab6089a'
down_revision = None
branch_labels = None
depends_on = None

import os, sys
from alembic import op
import sqlalchemy as sa


# Add plenario's root directory to the working path.
pwd = os.path.dirname(os.path.realpath(__file__))
plenario_path = os.path.join(pwd, '../../..')
sys.path.append(str(plenario_path))


def upgrade():
    op.drop_column('meta_master', 'is_socrata_source')
    op.drop_column('meta_master', 'business_key')
    op.drop_column('meta_master', 'contributed_data_types')
