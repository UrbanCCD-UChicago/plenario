from plenario.sensor_network.sensor_models import Observation
from plenario.database import redshift_session as session
from plenario.database import redshift_Base as Base
from plenario.database import redshift_engine as engine
from plenario.database import REDSHIFT_CONN as CONN
import random
import datetime

observations = []
for i in range(0, 1800):
    observations.append(Observation(
        node_id='ArrayOfThings' + str(random.randrange(0, 30)),
        datetime=datetime.datetime(2016, 4, 1, 14, 14, 14) + datetime.timedelta(seconds=random.randrange(0, 7776000)),
        temperature_temperature=random.randrange(-40, 125),
        atmosphericPressure_atmosphericPressure=random.randrange(300, 1100),
        relativeHumidity_relativeHumidity=random.randrange(0, 100),
        lightIntensity_lightIntensity=random.randrange(0, 124),
        acceleration_X=random.randrange(0, 8),
        acceleration_Y=random.randrange(0, 8),
        acceleration_Z=random.randrange(0, 8),
        instantaneousSoundSample_instantaneousSoundSample=random.randrange(0, 121),
        magneticFieldIntensity_X=random.randrange(0, 8),
        magneticFieldIntensity_Y=random.randrange(0, 8),
        magneticFieldIntensity_Z=random.randrange(0, 8),
        concentrationOf_SO2=random.randrange(0, 20),
        concentrationOf_H2S=random.randrange(0, 20),
        concentrationOf_O3=random.randrange(0, 20),
        concentrationOf_NO2=random.randrange(0, 20),
        concentrationOf_CO=random.randrange(0, 20),
        concentrationOf_reducingGases=random.randrange(0, 20),
        concentrationOf_oxidizingGases=random.randrange(0, 20),
        particulateMatter_PM1=random.randrange(0, 20),
        particulateMatter_PM2_5=random.randrange(0, 20),
        particulateMatter_PM10=random.randrange(0, 20),
    ))

print CONN
# Base.metadata.create_all(engine)

for obs in observations:
    session.add(obs)
session.commit()

for i in session.query().all():
    print i.temperature_temperature
