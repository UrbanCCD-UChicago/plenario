from collections import defaultdict
from plenario.database import session
from plenario.sensor_network.sensor_models import Sensor, NodeMeta, NetworkMeta
from wtforms import ValidationError


def validate_foi(foi_name, observed_properties):
    if not observed_properties:
        raise ValidationError("No observed properties were provided!")

    sensors = defaultdict(list)
    for sensor in session.query(Sensor).all():
        for prop in sensor.observed_properties:
            key, value = prop.split(".")
            sensors[key].append(value)

    if foi_name not in sensors:
        raise ValidationError("Bad FOI name, doesn't correspond to any sensor properties")

    if not all(prop["name"] in sensors[foi_name] for prop in observed_properties):
        raise ValidationError("Bad property specified!")


def validate_node(network):
    if network not in [net.name for net in session.query(NetworkMeta).all()]:
        raise ValidationError("Invalid network name!")
