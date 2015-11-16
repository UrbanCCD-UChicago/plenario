"""rename_point_id_and_date

Revision ID: 4e960796230e
Revises:
Create Date: 2015-11-19 10:28:48.518544

"""

# revision identifiers, used by Alembic.
revision = '4e960796230e'
down_revision = None
branch_labels = None
depends_on = None
import os, sys

pwd = os.path.dirname(os.path.realpath(__file__))
plenario_path = os.path.join(pwd, '../../..')
print plenario_path
sys.path.append(str(plenario_path))

from alembic import op
from plenario.models import MetaTable
from plenario.database import session
import sqlalchemy as sa


def dataset_names_with_date_col_names():
    return [(row.dataset_name, row.observed_date)
            for row in session.query(MetaTable.dataset_name, MetaTable.observed_date)
            .filter_by(approved_status='true').all()]


def upgrade():
    # Find oddballs that are doubly approved
    sel = sa.select([MetaTable.dataset_name])\
        .where(MetaTable.approved_status == 'true')\
        .group_by(MetaTable.dataset_name)\
        .having(sa.func.count(MetaTable.dataset_name) > 1)
    bad_names = [row.dataset_name for row in session.execute(sel)]

    # For now, (while I'm just testing the changes out)
    # Only remove the records from MetaMaster.
    # Leave the source tables intact.
    delete = sa.delete(MetaTable)\
        .where(MetaTable.dataset_name.in_(bad_names))
    session.execute(delete)

    #print session.query(MetaTable).first()

    for dataset_name, date_col in dataset_names_with_date_col_names():
        table_name = 'dat_' + dataset_name

        # Make foo_row_id -> point_id for all point datasets foo
        op.alter_column(
            table_name,
            '{}_row_id'.format(dataset_name),
            new_column_name='point_id'
        )
        # Make foo's date col -> point_date
        op.alter_column(
            table_name,
            unicode.lower(date_col),
            new_column_name='point_date'
        )


def downgrade():
    # Make point_id -> foo_row_id for all point datasets foo
    for dataset_name, date_col in dataset_names_with_date_col_names():
        table_name = 'dat_' + dataset_name
        op.alter_column(
            table_name,
            'point_id',
            new_column_name='{}_row_id'.format(dataset_name)
        )
        op.alter_column(
            table_name,
            'point_date',
            new_column_name=unicode.lower(date_col)
        )