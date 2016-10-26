from collections import defaultdict

from wtforms import ValidationError

from plenario.database import session
from plenario.models.SensorNetwork import FeatureMeta
from plenario.models.SensorNetwork import NetworkMeta


def validate_sensor_properties(observed_properties):
    if not observed_properties:
        raise ValidationError("No observed properties were provided!")

    features = defaultdict(list)
    for feature in session.query(FeatureMeta).all():
        for property_dict in feature.observed_properties:
            features[feature.name].append(property_dict["name"])

    for feature_property in observed_properties.values():
        feat, prop = feature_property.split(".")
        if feat not in features:
            raise ValidationError('Bad FOI name: "{}"'.format(feat))
        if prop not in features[feat]:
            raise ValidationError('Bad property name: "{}"'.format(prop))


def assert_json_enclosed_in_brackets(json_list):
    if type(json_list) != list:
        raise ValidationError("JSON must be enclosed in brackets: [ {...} ]")


def validate_node(network):
    if network not in [net.name for net in session.query(NetworkMeta).all()]:
        raise ValidationError("Invalid network name!")


def map_to_redshift_type(property_dict):
    """Given a dictionary of the form {"name": "foo", "value": "bar"}, pass
    or coerce the "value" strings to one of four types: BOOLEAN, DOUBLE
    PRECISION, BIGINT, VARCHAR.

    :param property_dict: contains apiary provided column definition
    :raises: ValidationError: if a provided value is unmappable"""

    redshift_type_map = {
        "BOOL": "BOOLEAN",
        "INT": "BIGINT",
        "INTEGER": "BIGINT",
        "DOUBLE": "DOUBLE PRECISION",
        "FLOAT": "DOUBLE PRECISION",
        "STRING": "VARCHAR"
    }

    value = property_dict["type"].upper()
    type_aliases = set(redshift_type_map.keys())
    type_standards = set(redshift_type_map.values())

    if value not in type_standards:
        if value not in type_aliases:
            raise ValidationError("Invalid type provided: {}".format(value))
        else:
            property_dict["value"] = redshift_type_map[value]
