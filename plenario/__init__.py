from flask import Flask, render_template, redirect, url_for, request
from flask_admin.contrib.sqla import ModelView
from flask_sqlalchemy import SQLAlchemy
from raven.contrib.flask import Sentry

# Unless PLENARIO_SENTRY_URL specified in settings, don't try to start raven.
from plenario.settings import PLENARIO_SENTRY_URL
from plenario.settings import DATABASE_CONN

sentry = None
if PLENARIO_SENTRY_URL:
    sentry = Sentry(dsn=PLENARIO_SENTRY_URL)

db = SQLAlchemy()
# NOTE: Models must be imported after initializing the db
# object since the models themselves need to import db.
from sensor_network.sensor_models import FeatureOfInterest, Sensor
from sensor_network.sensor_models import NetworkMeta, NodeMeta

def create_app():
    # API depends on the tables in the database to exist.
    # Don't import until we really need it to create the app
    # Since otherwise it may be called before init_db.py runs.
    from plenario.api import api, cache
    from plenario.admin import admin

    # These other imports might eventually use API as well.
    # plenario.views does now. So we'll put them here like
    # API and not import them until they're really needed.
    from plenario.database import session as db_session
    from plenario.models import bcrypt
    from plenario.auth import auth, login_manager
    from plenario.views import views
    from plenario.utils.helpers import slugify as slug

    app = Flask(__name__)
    app.config.from_object('plenario.settings')
    app.url_map.strict_slashes = False
    # login_manager.init_app(app)
    # login_manager.login_view = "auth.login"
    bcrypt.init_app(app)

    if sentry:
        sentry.init_app(app)
    app.register_blueprint(api)
    app.register_blueprint(views)
    # app.register_blueprint(auth)
    cache.init_app(app)

    app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_CONN

    db.init_app(app)
    with app.app_context():
        db.create_all()

    admin.init_app(app)
    admin.add_view(ModelView(FeatureOfInterest, db.session))
    admin.add_view(ModelView(Sensor, db.session))
    admin.add_view(ModelView(NetworkMeta, db.session))
    admin.add_view(ModelView(NodeMeta, db.session))

    @app.before_request
    def check_maintenance_mode():
        """
        If maintenance mode is turned on in settings.py,
        Disable the API and the interactive pages in the explorer.
        """
        maint = app.config.get('MAINTENANCE')
        maint_pages = ['/v1/api', '/explore', '/admin']

        maint_on = False
        for m in maint_pages:
            if m in request.path:
                maint_on = True

        if maint and maint_on and request.path != url_for('views.maintenance'):
            return redirect(url_for('views.maintenance'))

    @app.teardown_appcontext
    def shutdown_session(exception=None):
        db_session.remove()

    @app.errorhandler(404)
    def page_not_found(e):
        return render_template('404.html'), 404

    @app.errorhandler(500)
    def page_not_found(e):
        return render_template('error.html'), 500

    @app.template_filter('slugify')
    def slugify(s):
        return slug(s)

    @app.template_filter('format_number')
    def reverse_filter(s):
        return '{:,}'.format(s)

    @app.template_filter('format_date_sort')
    def reverse_filter(s):
        if s:
            return s.strftime('%Y%m%d%H%M')
        else:
            return '0'

    @app.template_filter('has_description')
    def has_description(list_of_cols):
        try:
            # Any description attribute filled?
            return any([col['description'] for col in list_of_cols])
        except KeyError:
            # Is there even a description attribute?
            return False

    return app


from plenario.database import session as db_session
# from plenario.auth import auth, login_manager
from plenario.models import bcrypt
from plenario.views import views
from plenario.utils.helpers import slugify as slug
