"""Script for migrating the contents of celery_taskmeta into etl_task."""

from plenario.settings import DATABASE_CONN

from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError, ProgrammingError

import traceback

engine = create_engine(DATABASE_CONN)

# rp = engine.execute("""
#     SELECT m.*, c.status, c.task_id, c.date_done, c.traceback
#     FROM meta_master AS m
#     LEFT JOIN celery_taskmeta AS c
#       ON c.id = (
#         SELECT id FROM celery_taskmeta
#         WHERE task_id = ANY(m.result_ids)
#         ORDER BY date_done DESC
#         LIMIT 1
#       )
#     WHERE m.approved_status = 'true'""")

# for row in rp.fetchall():
#     try:
#         engine.execute(
#             """
#             insert into etl_task (dataset_name, date_done, status, error, type)
#             values ('{}', '{}', '{}', '{}', '{}')
#             """.format(row.dataset_name, row.date_done, row.status, row.traceback, 'master')
#         )
#     except (IntegrityError, ProgrammingError):
#         traceback.print_exc()
#         pass

rp = engine.execute("""
    select * from meta_shape as ms natural join celery_taskmeta as ct
    where ms.celery_task_id = ct.task_id
""")

for row in rp.fetchall():
    try:
        engine.execute(
            """
            insert into etl_task (dataset_name, date_done, status, error, type)
            values ('{}', '{}', '{}', '{}', '{}')
            """.format(row.dataset_name, row.date_done, row.status, row.traceback, 'shape')
        )
    except (IntegrityError, ProgrammingError):
        traceback.print_exc()
        pass

