from flask_admin.contrib.sqla import ModelView
from wtforms import StringField

from plenario.apiary.validators import validate_foi, validate_node
from plenario.database import session
from plenario.sensor_network.sensor_models import NetworkMeta

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
        try:
            network = form.sensor_network.data
            validate_node(network)
            network_obj = session.query(NetworkMeta).filter(NetworkMeta.name == network)
            network_obj = network_obj.first()
            network_obj.nodes.append(model)
            session.commit()
        except:
            session.rollback()


class FOIMetaView(BaseMetaView):
    column_list = ("name", "observed_properties", "info")
    form_extra_fields = {
        "name": StringField("Name"),
        "Info": StringField("Info"),
    }

    def on_model_change(self, form, model, is_created):
        validate_foi(form.name.data, form.observed_properties.data)


admin_views = {
    "Sensor": BaseMetaView,
    "FOI": FOIMetaView,
    "Network": NetworkMetaView,
    "Node": NodeMetaView,
}
