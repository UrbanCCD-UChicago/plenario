from flask import redirect, url_for, request
from flask_admin.contrib.sqla import ModelView
from flask_admin.form.rules import Field
from flask_login import current_user
from wtforms import StringField

from plenario.database import session
from plenario.sensor_network.redshift_ops import create_foi_table
from plenario.sensor_network.redshift_ops import table_exists
from plenario.sensor_network.sensor_models import NetworkMeta
from validators import validate_node, validate_sensor_properties


# Based off a solution provided here:
# http://stackoverflow.com/questions/21727129
class CustomizableField(Field):
    def __init__(self, field_name, render_field='lib.render_field', field_args=None):
        if field_args is None:
            field_args = {}
        super(CustomizableField, self).__init__(field_name, render_field)
        self.extra_field_args = field_args

    def __call__(self, form, form_opts=None, field_args=None):
        if field_args is None:
            field_args = {}
        field_args.update(self.extra_field_args)
        return super(CustomizableField, self).__call__(form, form_opts, field_args)


class BaseMetaView(ModelView):
    can_delete = False
    column_display_pk = True
    form_extra_fields = {"name": StringField("Name")}

    def is_accessible(self):
        return current_user.is_authenticated
    
    def inaccessible_callback(self, name, **kwargs):
        return redirect(url_for('auth.login'))


class NetworkMetaView(BaseMetaView):
    column_list = ("name", "nodes", "info")

    form_edit_rules = [
        CustomizableField('name', field_args={'readonly': True}),
        CustomizableField('nodes', field_args={'readonly': True}),
        CustomizableField('info', field_args=None)
    ]


class NodeMetaView(BaseMetaView):
    can_edit = True
    column_list = ("id", "sensor_network", "location", "sensors", "info")
    form_extra_fields = {
        "location": StringField("Location"),
        "sensor_network": StringField("Network"),
        "id": StringField("ID"),
    }

    form_edit_rules = [
        CustomizableField('id', field_args={'readonly': True}),
        CustomizableField('sensor_network', field_args={'readonly': True}),
        CustomizableField('location', field_args={'readonly': True}),
        CustomizableField('sensors', field_args=None),
        CustomizableField('info', field_args=None)
    ]

    def on_model_change(self, form, model, is_created):
        network = form.sensor_network.data
        validate_node(network)
        network_obj = session.query(NetworkMeta).filter(NetworkMeta.name == network)
        network_obj = network_obj.first()
        network_obj.nodes.append(model)
        session.commit()


class FOIMetaView(BaseMetaView):
    can_edit = True
    can_delete = True
    column_list = ("name", "observed_properties", "info")
    form_extra_fields = {
        "name": StringField("Name"),
        "info": StringField("Info"),
    }

    form_edit_rules = [
        CustomizableField('name', field_args={'readonly': True}),
        CustomizableField('observed_properties', field_args=None),
        CustomizableField('info', field_args=None)
    ]

    def on_model_change(self, form, model, is_created):
        name = form.name.data
        properties = form.observed_properties.data
        if not table_exists(name):
            foi_properties = [{"name": e["name"], "type": e["type"]} for e in properties]
            create_foi_table(name, foi_properties)


class SensorMetaView(BaseMetaView):
    form_edit_rules = [
        CustomizableField('name', field_args={'readonly': True}),
        CustomizableField('observed_properties', field_args=None),
        CustomizableField('info', field_args=None)
    ]

    def on_model_change(self, form, model, is_created):
        validate_sensor_properties(form.observed_properties.data)


admin_views = {
    "Sensor": SensorMetaView,
    "FOI": FOIMetaView,
    "Network": NetworkMetaView,
    "Node": NodeMetaView,
}
