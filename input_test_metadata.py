from plenario.sensor_network.sensor_models import NetworkMeta, NodeMeta, FeatureOfInterest
from plenario.database import session, Base, app_engine
from geoalchemy2.elements import WKTElement
import random


aot = NetworkMeta(name='ArrayOfThings', nodeMetadata={
    "height": "meters",
    "direction": "Cardinal directions. One of N, NE, E, SE, S, SW, W, NW"
})

ios = NetworkMeta(name='InternetOfStuff', nodeMetadata={
    "height": "inches",
    "direction": "Cardinal directions. One of waffles"
})

nodes = []
for i in range(1, 31):
    nodes.append(NodeMeta(id='ArrayOfThings'+str(i),
                          sensorNetwork='ArrayOfThings',
                          location=WKTElement('POINT('+str(random.randrange(-5,5))+' '+str(random.randrange(-5,5))+')', srid=4326),
                          version=random.randrange(3,8),
                          procedures={
                              "temperature":{
                                  "sensors":[
                                      {
                                          "sensorType":"temperature sensor DS18B20+",
                                          "datasheet":"arrayofthings.github.io/datasheets/DS18B20"
                                      },
                                      {
                                          "sensorType":"temperature sensor TMP36",
                                          "datasheet":"arrayofthings.github.io/datasheets/TMP36"
                                      }
                                  ]
                              },
                              "numPeople":{
                                  "sensors":[
                                      {
                                          "sensorType":"OV7670 300KP camera",
                                          "datasheet":"arrayofthings.github.io/datasheets/OV7670"
                                      }
                                  ],
                                  "algorithms":[
                                      {
                                          "algorithm":"Szeliski 2.5.46",
                                          "datasheet":"arrayofthings.github.io/datasheets/Szeliski"
                                      }
                                  ]
                              }
                          }))

iosnode = NodeMeta(id='InternetOfStuff1',
                   sensorNetwork='InternetOfStuff',
                   location=WKTElement(
                       'POINT(' + str(random.randrange(-5, 5)) + ' ' + str(random.randrange(-5, 5)) + ')',
                       srid=4326),
                   version=random.randrange(3, 8),
                   procedures={
                       "humidity": {
                           "sensors": [
                               {
                                   "sensorType": "BAD",
                                   "datasheet": "FAKE"
                               },
                               {
                                   "sensorType": "BAD",
                                   "datasheet": "STUFF"
                               }
                           ]
                       },
                   })

temp = FeatureOfInterest(name='temperature',
                         sensorNetwork='ArrayOfThings',
                         observedProperties={'observedProperties':[
                             {
                                 "name": "temperature",
                                 "type": "numeric",
                                 "unit": "degrees Fahrenheit",
                                 "description": "accurate within +- .5 degrees Fahrenheit"
                             }
                         ]})

mag_field = FeatureOfInterest(name='magneticField',
                              sensorNetwork='ArrayOfThings',
                              observedProperties={'observedProperties': [
                                  {
                                      "name": "X",
                                      "type": "numeric",
                                      "unit": "Gauss",
                                      "description": "accurate within +- .002 Gauss"
                                  },
                                  {
                                      "name": "Y",
                                      "type": "numeric",
                                      "unit": "Gauss",
                                      "description": "accurate within +- .002 Gauss"
                                  },
                                  {
                                      "name": "Z",
                                      "type": "numeric",
                                      "unit": "Gauss",
                                      "description": "accurate within +- .002 Gauss"
                                  }
                              ]})

atm = FeatureOfInterest(name='atmosphericPressure',
                         sensorNetwork='ArrayOfThings',
                         observedProperties={'observedProperties': [
                             {
                                 "name": "pressure",
                                 "type": "numeric",
                                 "unit": "atms",
                                 "description": "accurate"
                             }
                         ]})

hum = FeatureOfInterest(name='humidity',
                        sensorNetwork='InternetOfStuff',
                        observedProperties={'observedProperties': [
                            {
                                "name": "humidity",
                                "type": "numeric",
                                "unit": "relativistic speeds",
                                "description": "wubalubadubdub"
                            }
                        ]})


# Base.metadata.create_all(app_engine)
# session.add(aot)
# session.add(ios)
# session.commit()
#
# for node in nodes:
#     session.add(node)
#     node.featuresOfInterest.append(temp)
#     node.featuresOfInterest.append(mag_field)
# session.add(iosnode)
# iosnode.featuresOfInterest.append(hum)
# session.commit()
#
# session.add(temp)
# session.add(mag_field)
# session.commit()
#
# for i in session.query(NetworkMeta).all()[0].nodes:
#     print i.id

session.add(atm)
session.commit()

