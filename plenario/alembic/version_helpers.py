from plenario.database import session
from plenario.models import MetaTable


def dataset_names():
    return [row.dataset_name for row in session.query(MetaTable.dataset_name)
            .filter_by(approved_status='true').all()]