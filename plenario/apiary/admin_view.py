from flask_admin.contrib.sqla import ModelView
from wtforms import StringField

from plenario.database import session
from plenario.sensor_network.sensor_models import NetworkMeta
from plenario.sensor_network.redshift_ops import create_foi_table, add_column
from plenario.sensor_network.redshift_ops import table_exists

from validators import validate_node, validate_sensor_properties

class BaseMetaView(ModelView):
    can_delete = False
    can_edit = False
    column_display_pk = True
    form_extra_fields = {"name": StringField("Name")}


class NetworkMetaView(BaseMetaView):
    column_list = ("name", "nodes", "info")


class NodeMetaView(BaseMetaView):
    can_edit = True
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
    can_delete = True
    column_list = ("name", "observed_properties", "info")
    form_extra_fields = {
        "name": StringField("Name"),
        "Info": StringField("Info"),
    }

    def on_model_change(self, form, model, is_created):
        name = form.name.data
        properties = form.observed_properties.data
        if not table_exists(name):
            foi_properties = [{"name": e["name"], "type": e["type"]} for e in properties]
            create_foi_table(name, foi_properties)


class SensorMetaView(BaseMetaView):

    def on_model_change(self, form, model, is_created):
        validate_sensor_properties(form.observed_properties.data)
        

admin_views = {
    "Sensor": SensorMetaView,
    "FOI": FOIMetaView,
    "Network": NetworkMetaView,
    "Node": NodeMetaView,
}
