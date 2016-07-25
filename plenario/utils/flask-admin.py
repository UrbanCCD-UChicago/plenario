from flask import Flask, render_template, request, url_for, redirect
from flask_admin import Admin, helpers as admin_helpers, AdminIndexView
from flask_admin.contrib.sqla import ModelView

from flask_sqlalchemy import SQLAlchemy
from flask_security import Security, SQLAlchemyUserDatastore, current_user
from flask_security import UserMixin, RoleMixin, login_required
from flask_security.utils import encrypt_password

from runserver import application as app

# Extra configurations that will need to happen on the app.
app.config['DEBUG'] = True
app.config['SECRET_KEY'] = 'the most secret of keys'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True

# Setup a database connection object for the app.
db = SQLAlchemy(app)


roles_trainers = db.Table(
    'roles_trainers',
    db.Column('trainer_id', db.Integer(), db.ForeignKey('trainer.id')),
    db.Column('role_id', db.Integer(), db.ForeignKey('role.id'))
)


class Role(db.Model, RoleMixin):
    id = db.Column(db.Integer(), primary_key=True)
    name = db.Column(db.String(64), unique=True, nullable=False)
    decription = db.Column(db.String(256))


class Trainer(db.Model, UserMixin):
    id = db.Column(db.Integer(), primary_key=True)
    email = db.Column(db.String(64), nullable=False, unique=True)
    password = db.Column(db.String(256))
    active = db.Column(db.Boolean)

    roles = db.relationship('Role',
                            secondary=roles_trainers,
                            backref=db.backref('trainers', lazy='dynamic'))


class Pokemon(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String, unique=True, nullable=False)

    def __repr__(self):
        return "<Pokemon #{}: {}>".format(self.id, self.name)


# Setup Flask-Security.
user_datastore = SQLAlchemyUserDatastore(db, Trainer, Role)
security = Security(app, user_datastore)


# Integrate a ModelView with Flask-Security.
class PokeView(AdminIndexView):

    def is_accessible(self):
        return current_user.has_role('admin')

    # When a user tries to access a page they don't have permissions for.
    def inaccessible_callback(self, name, **kwargs):
        return redirect(url_for('security.login', next=request.url))


# Setup admin views for the app.
admin = Admin(app,
              name='FlaskPokemon',
              template_mode='bootstrap3',
              index_view=PokeView())
admin.add_view(ModelView(Trainer, db.session))
admin.add_view(ModelView(Pokemon, db.session))


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


@app.route('/')
def home():
    return "FlaskPokemon Homepage"


@app.route('/login')
@login_required
def admin():
    return redirect('/admin')


@app.before_first_request
def setup():
    db.create_all()

    user_datastore.find_or_create_role(name='admin')
    if not user_datastore.get_user('admin@example.com'):
        user_datastore.create_user(
            email='admin@example.com',
            password=encrypt_password('password')
        )
    db.session.commit()

    user_datastore.add_role_to_user('admin@example.com', 'admin')
    db.session.commit()

if __name__ == '__main__':
    app.run()
