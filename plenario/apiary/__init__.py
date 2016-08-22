from plenario.database import session
from plenario.sensor_network.sensor_models import FeatureOfInterest
from plenario.sensor_network.sensor_models import NetworkMeta, NodeMeta, Sensor

from admin import admin
from admin_view import admin_views
from views import blueprint

admin.add_view(admin_views["FOI"](FeatureOfInterest, session))
admin.add_view(admin_views["Sensor"](Sensor, session))
admin.add_view(admin_views["Network"](NetworkMeta, session))
admin.add_view(admin_views["Node"](NodeMeta, session))

apiary = admin  
apiary_bp = blueprint
