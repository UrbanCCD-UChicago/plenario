from flask import redirect
from flask_admin import Admin, AdminIndexView, expose
from flask_admin.menu import url_for
from flask_login import current_user

from plenario.database import postgres_session
from plenario.models.SensorNetwork import FeatureMeta, NetworkMeta, NodeMeta, SensorMeta
from .admin_views import admin_views
from .views import blueprint, index


class ApiaryIndexView(AdminIndexView):
    def is_accessible(self):
        return current_user.is_authenticated

    def inaccessible_callback(self, name, **kwargs):
        return redirect(url_for('auth.login'))

    @expose('/')
    def index(self):
        try:
            return self.render('apiary/index.html', elements=index())
        except KeyError:
            return self.render('apiary/index.html', elements=[])


admin = Admin(
    index_view=ApiaryIndexView(url='/apiary'),
    name='Plenario',
    template_mode='bootstrap3',
    url='/apiary',
)

admin.add_view(admin_views['FOI'](FeatureMeta, postgres_session))
admin.add_view(admin_views['Sensor'](SensorMeta, postgres_session))
admin.add_view(admin_views['Network'](NetworkMeta, postgres_session))
admin.add_view(admin_views['Node'](NodeMeta, postgres_session))

apiary = admin
apiary_bp = blueprint
