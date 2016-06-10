from plenario.settings import DATABASE_CONN
from plenario.database import Base
from plenario.models import MetaTable
from sqlalchemy import create_engine, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import NoSuchTableError


def main():

    # establish connection to provided database
    engine = create_engine(DATABASE_CONN, convert_unicode=True)
    session = sessionmaker(bind=engine)()

    # grab the MetaTable records
    query = session.query(MetaTable)

    for table in query.all():

        try:
            # reflect existing table information into a Table object
            t = Table(table.dataset_name, Base.metadata, autoload=True, extend_existing=True)
            print(table)

            cols = {}

            for col in t.columns:
                c_name = str(col.name)
                c_type = str(col.type)

                if c_name not in {u'geom', u'point_date', u'hash'}:
                    cols[c_name] = c_type

            # update existing table
            table.column_names = cols

            session.commit()

        except NoSuchTableError:
            pass

        print('... done.')


if __name__ == '__main__':

    print "Connecting to {}".format(DATABASE_CONN)
    main()
