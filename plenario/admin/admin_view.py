import pdb

from flask_admin.contrib.sqla import ModelView
from wtforms import StringField, validators
from BeautifulSoup import BeautifulSoup
from copy import deepcopy
from ppretty import ppretty

from plenario.apiary.validators import validate_foi


class BaseMetaView(ModelView):
    can_delete = False
    can_edit = False
    column_display_pk = True
    form_extra_fields = {"name": StringField("Name")}


class NetworkMetaView(BaseMetaView):
    column_list = ("name", "nodes", "info")


class NodeMetaView(BaseMetaView):
    column_list = ("id", "sensor_network", "location", "sensors", "info")
    form_extra_fields = {}


class FOIView(BaseMetaView):
    can_delete = True

    column_list = ("name", "observed_properties", "info")

    form_extra_fields = {
        "name": StringField("Name"),
        "Info": StringField("Info"),
    }

    def on_model_change(self, form, model, is_created):

        validate_foi(form.name.data, form.observed_properties.data)


admin_views = {
    "Base": BaseMetaView,
    "FOI": FOIView,
    "Network": NetworkMetaView,
    "Node": NodeMetaView,
}
