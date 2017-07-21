#!/usr/bin/env python

"""Script for migrating the contents of celery_taskmeta into etl_task."""

import traceback

from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError, ProgrammingError

from plenario.settings import DATABASE_CONN


engine = create_engine(DATABASE_CONN)


def main():
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


if __name__ == '__main__':
    main()
