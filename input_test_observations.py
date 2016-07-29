from plenario.sensor_network.redshift_ops import insert_observation
from plenario.database import redshift_session as session
from plenario.database import redshift_Base as Base
from plenario.database import redshift_engine as engine
from plenario.database import REDSHIFT_CONN as CONN
import random
import datetime


feature_list = ['temperature','atmosphericPressure']
sensor_list = ['TMP112','HUTY565','XGSF98','KLOO0','WUBA22']

for i in range(0, 10000):
    insert_observation('arrayofthings',
                       'ArrayOfThings' + str(random.randrange(1, 31)),
                       (datetime.datetime(2016, 4, 1, 14, 14, 14) +
                        datetime.timedelta(seconds=random.randrange(0, 7776000))).isoformat().split('+')[0],
                       feature_list[random.randrange(0, 2)],
                       sensor_list[random.randrange(0, 5)],
                       random.randrange(0, 100)
    )

for i in range(0, 3000):
    insert_observation('arrayofthings',
                       'ArrayOfThings' + str(random.randrange(1, 31)),
                       (datetime.datetime(2016, 4, 1, 14, 14, 14) +
                        datetime.timedelta(seconds=random.randrange(0, 7776000))).isoformat().split('+')[0],
                       'magneticField',
                       'BMP340',
                       random.randrange(0, 100),
                       random.randrange(0, 100),
                       random.randrange(0, 100),
    )

print CONN

