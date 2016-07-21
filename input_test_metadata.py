from plenario.sensor_network.sensor_models import NetworkMeta, NodeMeta
from plenario.database import session, Base, app_engine
from geoalchemy2.elements import WKTElement
import random


aot = NetworkMeta(name='ArrayOfThings', nodeMetadata={
    "height": "meters",
    "direction": "Cardinal directions. One of N, NE, E, SE, S, SW, W, NW"
}, featuresOfInterest=["temperature","numPeople"])

ios = NetworkMeta(name='InternetOfStuff', nodeMetadata={
    "height": "inches",
    "direction": "Cardinal directions. One of waffles"
}, featuresOfInterest=[])

nodes = []
for i in range(0,30):
    nodes.append(NodeMeta(id='ArrayOfThings'+str(i),
                          sensorNetwork='ArrayOfThings',
                          location=WKTElement('POINT('+str(random.randrange(-5,5))+' '+str(random.randrange(-5,5))+')', srid=4326),
                          version=random.randrange(3,8),
                          featuresOfInterest=["temperature","numPeople"],
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


Base.metadata.create_all(app_engine)
session.add(aot)
session.add(ios)
session.commit()
for node in nodes:
    session.add(node)
session.commit()

for i in session.query(NetworkMeta).all()[0].nodes:
    print i.id


