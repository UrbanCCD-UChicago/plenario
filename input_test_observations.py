from plenario.sensor_network.redshift_ops import insert_observation
import random
import json
import datetime

sensor_list = ['TMP112', 'BMP340']

# for i in range(0, 500):
#     insert_observation('temperature',
#                        'ArrayOfThings' + str(random.randrange(1, 31)),
#                        (datetime.datetime(2016, 4, 1, 14, 14, 14) +
#                         datetime.timedelta(seconds=random.randrange(0, 7776000))).isoformat().split('+')[0],
#                        sensor_list[random.randrange(0, 2)],
#                        [random.randrange(0, 100), ],
#                        random.randrange(1000, 4000)
#                        )
#
# for i in range(0, 500):
#     insert_observation('humidity',
#                        'ArrayOfThings' + str(random.randrange(1, 31)),
#                        (datetime.datetime(2016, 4, 1, 14, 14, 14) +
#                         datetime.timedelta(seconds=random.randrange(0, 7776000))).isoformat().split('+')[0],
#                        'BMP340',
#                        [random.randrange(0, 100), ],
#                        random.randrange(1000, 4000)
#                        )
#
# for i in range(0, 500):
#     insert_observation('magneticField',
#                        'ArrayOfThings' + str(random.randrange(1, 31)),
#                        (datetime.datetime(2016, 4, 1, 14, 14, 14) +
#                         datetime.timedelta(seconds=random.randrange(0, 7776000))).isoformat().split('+')[0],
#                        'UBQ120',
#                        [random.randrange(0, 100),
#                         random.randrange(0, 100),
#                         random.randrange(0, 100)],
#                        random.randrange(1000, 4000)
#                        )

with open('loadtemp1.json', 'w') as outfile:
    for i in range(0, 50000000):
        json.dump({"nodeid":"ArrayOfThings" + str(random.randrange(1, 31)),
                   "datetime": (datetime.datetime(2016, 4, 1, 14, 14, 14) +
                                datetime.timedelta(seconds=random.randrange(0, 7776000))).isoformat().split('+')[0].replace("T", " "),
                   "sensor": sensor_list[random.randrange(0, 2)],
                   "temperature": random.randrange(0, 100),
                   "procedures": random.randrange(1000, 4000)
                   }, outfile)

with open('loadhum1.json', 'w') as outfile:
    for i in range(0, 50000000):
        json.dump({"nodeid": "ArrayOfThings" + str(random.randrange(1, 31)),
                   "datetime": (datetime.datetime(2016, 4, 1, 14, 14, 14) +
                                datetime.timedelta(seconds=random.randrange(0, 7776000))).isoformat().split('+')[0].replace("T", " "),
                   "sensor": "BMP340",
                   "humidity": random.randrange(0, 100),
                   "procedures": random.randrange(1000, 4000)
                   }, outfile)

with open('loadmag1.json', 'w') as outfile:
    for i in range(0, 50000000):
        json.dump({"nodeid": "ArrayOfThings" + str(random.randrange(1, 31)),
                   "datetime": (datetime.datetime(2016, 4, 1, 14, 14, 14) +
                                datetime.timedelta(seconds=random.randrange(0, 7776000))).isoformat().split('+')[0].replace("T", " "),
                   "sensor": "UBQ120",
                   "x": random.randrange(0, 100),
                   "y": random.randrange(0, 100),
                   "z": random.randrange(0, 100),
                   "procedures": random.randrange(1000, 4000)
                   }, outfile)
