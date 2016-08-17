"""Script for migrating the contents of celery_taskmeta into etl_task."""

from plenario.settings import DATABASE_CONN

from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError, ProgrammingError

import traceback

engine = create_engine(DATABASE_CONN)


def migrate_meta_master():
    rp = engine.execute("""
        SELECT m.*, c.status, c.task_id, c.date_done, c.traceback
        FROM meta_master AS m
        LEFT JOIN celery_taskmeta AS c
          ON c.id = (
            SELECT id FROM celery_taskmeta
            WHERE task_id = ANY(m.result_ids)
            ORDER BY date_done DESC
            LIMIT 1
          )
        WHERE m.approved_status = 'true'""")

    for row in rp.fetchall():
        print row.dataset_name + "... ",
        try:
            engine.execute(
                """
                insert into etl_task (dataset_name, date_done, status, error, type)
                values ('{}', '{}', '{}', '{}', '{}')
                """.format(row.dataset_name, row.date_done, row.status, row.traceback, 'master')
            )
            print "ok"
        except (IntegrityError, ProgrammingError):
            print "fail"
            traceback.print_exc()
            pass


def migrate_meta_shape():
    rp = engine.execute("""
        select * from meta_shape as ms natural join celery_taskmeta as ct
        where ms.celery_task_id = ct.task_id
    """)

    for row in rp.fetchall():
        print row.dataset_name + "... ",
        try:
            engine.execute(
                """
                insert into etl_task (dataset_name, date_done, status, error, type)
                values ('{}', '{}', '{}', '{}', '{}')
                """.format(row.dataset_name, row.date_done, row.status, row.traceback, 'shape')
            )
            print "ok"
        except (IntegrityError, ProgrammingError):
            print "fail"
            traceback.print_exc()
            pass


if __name__ == '__main__':
    print "Connecting to {}.".format(DATABASE_CONN)
    migrate_meta_master()

    # 4 Failed to migrate.
    # real_estate_tax_balances
    # food_service_establishment_last_inspection
    # open_buisness_locations_san_francisco
    # wnv_mosquito_test_results
