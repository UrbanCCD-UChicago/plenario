from flask_admin.contrib.sqla import ModelView
from wtforms import StringField

from plenario.apiary.validators import validate_foi, validate_node
from plenario.apiary.validators import validate_sensor


class BaseMetaView(ModelView):
    can_delete = False
    can_edit = False
    column_display_pk = True
    form_extra_fields = {"name": StringField("Name")}


class NetworkMetaView(BaseMetaView):
    column_list = ("name", "nodes", "info")


class NodeMetaView(BaseMetaView):
    column_list = ("id", "sensor_network", "location", "sensors", "info")
    form_extra_fields = {
        "location": StringField("Location"),
        "sensor_network": StringField("Network"),
        "id": StringField("ID"),
    }

    def on_model_change(self, form, model, is_created):
        validate_node(form.sensor_network.data)


class FOIMetaView(BaseMetaView):
    column_list = ("name", "observed_properties", "info")
    form_extra_fields = {
        "name": StringField("Name"),
        "Info": StringField("Info"),
    }

    def on_model_change(self, form, model, is_created):
        validate_foi(form.name.data, form.observed_properties.data)


admin_views = {
    "Sensor": SensorMetaView,
    "FOI": FOIMetaView,
    "Network": NetworkMetaView,
    "Node": NodeMetaView,
}
