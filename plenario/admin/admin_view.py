from flask_admin.contrib.sqla import ModelView
from wtforms import StringField


class BaseMetaView(ModelView):

    can_delete = False
    can_edit = False
    column_display_pk = True
    form_extra_fields = { "name": StringField("Name") }


class NetworkMetaView(BaseMetaView):

    column_list = ("name", "nodes", "info")


class NodeMetaView(BaseMetaView):

    column_list = ("id", "sensorNetwork", "location", "sensors", "info")
    form_extra_fields = {}


class FOIView(BaseMetaView):

    column_list = ("name", "observedProperties", "info")
    column_editable_list = ('observedProperties',)
    form_extra_fields = { 
        "name": StringField("Name"),
        "Info": StringField("Info"),
    }


admin_views = {
    "Base": BaseMetaView,
    "FOI": FOIView,
    "Network": NetworkMetaView,
    "Node": NodeMetaView,
}