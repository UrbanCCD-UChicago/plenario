from plenario.sensor_models import NetworkMeta, NodeMeta
from plenario.database import session, Base, app_engine

# aot = NetworkMeta(name='ArrayOfThings', nodeMetadata={
#     "height": "meters",
#     "direction": "Cardinal directions. One of N, NE, E, SE, S, SW, W, NW"
#   }, featuresOfInterest=["temperature","numPeople"])
#
# ios = NetworkMeta(name='InternetOfStuff', nodeMetadata={
#     "height": "inches",
#     "direction": "Cardinal directions. One of waffles"
#   }, featuresOfInterest=[])

# aot_node1 = NodeMeta(id='ArrayOfThings1', sensorNetwork='ArrayOfThings', location='POINT(-87.91372618 41.64625754)',
#                 version=7, featuresOfInterest=["temperature","numPeople"], procedures={
#         "temperature":{
#             "sensors":[
#                 {
#                     "sensorType":"temperature sensor DS18B20+",
#                     "datasheet":"arrayofthings.github.io/datasheets/DS18B20"
#                 },
#                 {
#                     "sensorType":"temperature sensor TMP36",
#                     "datasheet":"arrayofthings.github.io/datasheets/TMP36"
#                 }
#             ]
#         },
#             "numPeople":{
#                 "sensors":[
#                     {
#                         "sensorType":"OV7670 300KP camera",
#                         "datasheet":"arrayofthings.github.io/datasheets/OV7670"
#                     }
#                 ],
#                 "algorithms":[
#                     {
#                         "algorithm":"Szeliski 2.5.46",
#                         "datasheet":"arrayofthings.github.io/datasheets/Szeliski"
#                     }
#                 ]
#             }
#         })

# aot_node2 = NodeMeta(id='ArrayOfThings2', sensorNetwork='ArrayOfThings', location='POINT(-7.91728368 24.6461121)',
#                 version=8, featuresOfInterest=["temperature", "numPeople"], procedures={
#         "temperature": {
#             "sensors": [
#                 {
#                     "sensorType": "temperature sensor DS18B20+",
#                     "datasheet": "arrayofthings.github.io/datasheets/DS18B20"
#                 },
#                 {
#                     "sensorType": "temperature sensor TMP36",
#                     "datasheet": "arrayofthings.github.io/datasheets/TMP36"
#                 }
#             ]
#         },
#         "numPeople": {
#             "sensors": [
#                 {
#                     "sensorType": "OV7670 300KP camera",
#                     "datasheet": "arrayofthings.github.io/datasheets/OV7670"
#                 }
#             ],
#             "algorithms": [
#                 {
#                     "algorithm": "Szeliski 2.5.46",
#                     "datasheet": "arrayofthings.github.io/datasheets/Szeliski"
#                 }
#             ]
#         }
#     })

aot_node3 = NodeMeta(id='ArrayOfThings3', sensorNetwork='ArrayOfThings', location='POINT(-56.91372123 48.64625345)',
                     version=5, featuresOfInterest=["temperature", "numPeople"], procedures={
        "temperature": {
            "sensors": [
                {
                    "sensorType": "temperature sensor DS18B20+",
                    "datasheet": "arrayofthings.github.io/datasheets/DS18B20"
                },
                {
                    "sensorType": "temperature sensor TMP36",
                    "datasheet": "arrayofthings.github.io/datasheets/TMP36"
                }
            ]
        },
        "numPeople": {
            "sensors": [
                {
                    "sensorType": "OV7671 300KP camera",
                    "datasheet": "arrayofthings.github.io/datasheets/OV7671"
                }
            ],
            "algorithms": [
                {
                    "algorithm": "Szeliskito 2.5.46",
                    "datasheet": "arrayofthings.github.io/datasheets/Szeliskito"
                }
            ]
        }
    })

Base.metadata.create_all(app_engine)
# session.add(aot)
# session.add(ios)
# session.commit()
# session.add(aot_node1)
# session.add(aot_node2)
session.add(aot_node3)
session.commit()

for i in session.query(NetworkMeta).all()[0].nodes:
    print i.id
for i in session.query(NetworkMeta).all()[1].nodes:
    print i.id



