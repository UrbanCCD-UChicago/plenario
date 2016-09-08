from plenario.sensor_network.sensor_models import NetworkMeta, NodeMeta, FeatureOfInterest, Sensor
from plenario.database import session, Base, app_engine
from geoalchemy2.elements import WKTElement
import random

# aot = NetworkMeta(name='ArrayOfThings', info={
#     "website": "aot.org",
#     "contact": "aot@chicago.org"
# })
#
# ios = NetworkMeta(name='InternetOfStuff', info={
#     "website": "ios.org",
#     "contact": "ios@seattle.org"
# })

nodes = []
for i in range(1, 2):
    nodes.append(NodeMeta(id='026',
                          sensor_network='array_of_things',
                          location=WKTElement(
                              'POINT(41.8781 -87.6298)',
                              srid=4326),
                          info={}
                          ))

# iosnode = NodeMeta(id='InternetOfStuff1',
#                    sensor_network='InternetOfStuff',
#                    location=WKTElement(
#                        'POINT(' + str(random.randrange(-5, 5)) + ' ' + str(random.randrange(-5, 5)) + ')', srid=4326),
#                    info={
#                        "height": {
#                            "value": 5,
#                            "unit": "meters"
#                        },
#                        "orientation": {
#                            "value": "NE",
#                            "unit": "Cardinal directions. One of N, NE, E, SE, S, SW, W, NW"
#                        }
#                    })
#
# temp = FeatureOfInterest(name='temperature',
#                          observed_properties={'observed_properties': [
#                              {
#                                  "name": "temperature",
#                                  "type": "numeric",
#                                  "unit": "degrees Fahrenheit",
#                                  "description": "accurate within +- .5 degrees Fahrenheit"
#                              }
#                          ]})
#
# mag_field = FeatureOfInterest(name='magneticField',
#                               observed_properties={'observed_properties': [
#                                   {
#                                       "name": "X",
#                                       "type": "numeric",
#                                       "unit": "Gauss",
#                                       "description": "accurate within +- .002 Gauss"
#                                   },
#                                   {
#                                       "name": "Y",
#                                       "type": "numeric",
#                                       "unit": "Gauss",
#                                       "description": "accurate within +- .002 Gauss"
#                                   },
#                                   {
#                                       "name": "Z",
#                                       "type": "numeric",
#                                       "unit": "Gauss",
#                                       "description": "accurate within +- .002 Gauss"
#                                   }
#                               ]})
#
# atm = FeatureOfInterest(name='atmosphericPressure',
#                         observed_properties={'observed_properties': [
#                             {
#                                 "name": "pressure",
#                                 "type": "numeric",
#                                 "unit": "atms",
#                                 "description": "accurate"
#                             }
#                         ]})
#
# hum = FeatureOfInterest(name='humidity',
#                         observed_properties={'observed_properties': [
#                             {
#                                 "name": "humidity",
#                                 "type": "numeric",
#                                 "unit": "relativistic speeds",
#                                 "description": "wubalubadubdub"
#                             }
#                         ]})
#
tmp113 = Sensor(name='TMP113',
                observed_properties={"temperature": "temperature.temperature"},
                info={})
#
# bmp340 = Sensor(name='BMP340',
#                 properties=['temperature.temperature', 'humidity.humidity'],
#                 info={"datasheet": "BMP340.datashe.et"})
#
# ubq120 = Sensor(name='UBQ120',
#                 properties=['magneticField.X', 'magneticField.Y', 'magneticField.Z'],
#                 info={"datasheet": "UBQ120.datashe.et"})
#
# pre450 = Sensor(name='PRE450',
#                 properties=['atmosphericPressure.pressure', ],
#                 info={"datasheet": "PRE450.datashe.et"})

# # init db
# Base.metadata.create_all(app_engine)
#
# # networks
# session.add(aot)
# session.add(ios)
# session.commit()
#
# nodes
session.add(tmp113)
for node in nodes:
    node.sensors.append(tmp113)
    session.add(node)
session.commit()
#
# # foi
# session.add(temp)
# session.add(mag_field)
# session.add(atm)
# session.add(hum)
# aot.featuresOfInterest.append(temp)
# aot.featuresOfInterest.append(mag_field)
# aot.featuresOfInterest.append(hum)
# ios.featuresOfInterest.append(atm)
# ios.featuresOfInterest.append(temp)
# session.commit()
#
# # sensors
# session.add(tmp112)
# session.add(bmp340)
# session.add(ubq120)
# temp.sensors.append(tmp112)
# temp.sensors.append(bmp340)
# mag_field.sensors.append(ubq120)
# hum.sensors.append(bmp340)
# atm.sensors.append(pre450)
# tmp112.sensor_networks.append(aot)
# tmp112.sensor_networks.append(ios)
# bmp340.sensor_networks.append(aot)
# ubq120.sensor_networks.append(aot)
# pre450.sensor_networks.append(ios)
# session.commit()

