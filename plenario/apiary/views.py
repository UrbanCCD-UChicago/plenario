from json import dumps, loads

from flask import Blueprint
from redis import Redis
from sqlalchemy import desc, select

from plenario.database import redshift_base as rshift_base
from plenario.models.SensorNetwork import SensorMeta
from plenario.settings import REDIS_HOST

blueprint = Blueprint('apiary', __name__)
redis = Redis(REDIS_HOST)


def index() -> list:
    """Generate the information necessary for displaying unknown features on the
    admin index page.
    """
    rshift_base.metadata.reflect()
    unknown_features = rshift_base.metadata.tables['unknown_feature']

    query = select([unknown_features]) \
        .order_by(desc(unknown_features.c.datetime)) \
        .limit(5)
    rp = query.execute()

    results = []
    for row in rp:

        sensor = SensorMeta.query.get(row.sensor)

        if sensor is None:
            expected = 'No metadata exists for this sensor!'
        else:
            expected = dumps(sensor.observed_properties, indent=2, sort_keys=True)

        result = {
            'sensor': row.sensor,
            'datetime': row.datetime,
            'incoming': dumps(loads(row.data), indent=2, sort_keys=True, default=str),
            'expected': expected
        }

        results.append(result)

    return results
