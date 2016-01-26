"""Make approved_status boolean

Revision ID: 2fd372e85d95
Revises: 1bb31ab6089a
Create Date: 2016-01-26 09:43:47.584818

"""

# revision identifiers, used by Alembic.
revision = '2fd372e85d95'
down_revision = '1bb31ab6089a'
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

    str_to_bool = '''
    ALTER TABLE meta_master ADD COLUMN approved_bool BOOLEAN;
    UPDATE meta_master SET approved_bool=True WHERE approved_status='true';
    UPDATE meta_master SET approved_bool=False WHERE approved_status!='true';
    ALTER TABLE meta_master DROP COLUMN approved_status;
    ALTER TABLE meta_master RENAME COLUMN approved_bool TO approved_status;
    '''
    op.execute(str_to_bool)
