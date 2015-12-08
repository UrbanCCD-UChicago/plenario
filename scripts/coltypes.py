from sqlalchemy import Table, MetaData
from sqlalchemy.sql.sqltypes import DATE
from plenario.database import session, app_engine
from plenario.models import MetaTable
from sqlalchemy.exc import NoSuchTableError
from plenario.utils.helpers import slugify
from plenario.etl.point import PlenarioETL
import traceback

if __name__ == "__main__":
    them = session.query(MetaTable).all()
    meta = MetaData()
    for t in them:
        try:
            table = Table('dat_{0}'.format(t.dataset_name), meta, 
                autoload=True, autoload_with=app_engine, keep_existing=True)
            try:
               date_col = getattr(table.c, slugify(t.observed_date))
               if type(date_col.type) == DATE:
                   e = PlenarioETL(t.as_dict())
                   e._get_or_create_data_table()
                   e._add_weather_info()
                   print 'added weather for {0}'.format(t.dataset_name)
            except AttributeError, e:
                raise e
                print 'no col {0}'.format(t.observed_date)
                pass
        except NoSuchTableError:
            print 'no table {0}'.format(t.dataset_name) 
            pass
