from flask_admin import Admin, AdminIndexView

from plenario.database import session
from plenario.sensor_network.sensor_models import FeatureOfInterest
from plenario.sensor_network.sensor_models import NetworkMeta, NodeMeta, Sensor

from admin_view import admin_views
from views import blueprint

admin = Admin(
    # index_view=AdminIndexView(),  << this straight up seems to not work
    name='Plenario',
    template_mode='bootstrap3',
    url='/apiary',
)

admin.add_view(admin_views["FOI"](FeatureOfInterest, session))
admin.add_view(admin_views["Sensor"](Sensor, session))
admin.add_view(admin_views["Network"](NetworkMeta, session))
admin.add_view(admin_views["Node"](NodeMeta, session))

apiary = admin  
apiary_bp = blueprint
