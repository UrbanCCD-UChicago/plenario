from collections import defaultdict
from wtforms import ValidationError

from plenario.database import session
from plenario.sensor_network.sensor_models import FeatureOfInterest
from plenario.sensor_network.sensor_models import NetworkMeta


def validate_sensor_properties(observed_properties):
    if not observed_properties:
        raise ValidationError("No observed properties were provided!")

    features = defaultdict(list)
    for feature in session.query(FeatureOfInterest).all():
        for property_dict in feature.observed_properties:
            features[feature.name].append(property_dict["name"])

    for feature_property in observed_properties:
        feat, prop = feature_property.values()[0].split(".")
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
