from flask import request, url_for, redirect
from flask_admin import Admin, helpers as admin_helpers, AdminIndexView
from flask_admin.contrib.sqla import ModelView

from flask_security import current_user
from flask_security import UserMixin, RoleMixin, login_required
from flask_security.utils import encrypt_password


# Setup Flask-Security.
# user_datastore = SQLAlchemyUserDatastore(db, User, Role)
# security = Security(app, user_datastore)


# Integrate a ModelView with Flask-Security.
# class AdminView(AdminIndexView):
#
#     def is_accessible(self):
#         return current_user.has_role('admin')
#
#     # When a user tries to access a page they don't have permissions for.
#     def inaccessible_callback(self, name, **kwargs):
#         return redirect(url_for('security.login', next=request.url))


# Setup Flask-Admin.
admin = Admin(name='Plenario',
              template_mode='bootstrap3')
# index_view=AdminView())


# Define a context processor for mergin Flask-Admin's template
# context into the Flask-Security views. (?)
# @security.context_processor
# def security_context_processor():
#    return {
#        'admin_base_template': admin.base_template,
#        'admin_view': admin.index_view,
#        'h': admin_helpers,
#        'get_url': url_for
#    }

# @app.route('/')
# def home():
#     return "FlaskPokemon Homepage"
#
#
# @app.route('/login')
# @login_required
# def admin():
#     return redirect('/admin')
#
#
# @app.before_first_request
# def setup():
#     db.drop_all()
#     db.create_all()
#
#     user_datastore.find_or_create_role(name='admin')
#     if not user_datastore.get_user('admin@example.com'):
#         user_datastore.create_user(
#             username='administrator',
#             email='admin@example.com',
#             password=encrypt_password('password')
#         )
#     db.session.commit()
#
#     user_datastore.add_role_to_user('admin@example.com', 'admin')
#     db.session.commit()
#
# if __name__ == '__main__':
#     app.run()