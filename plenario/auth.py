from flask import Blueprint, render_template, redirect, request, url_for, \
    session as flask_session, flash
from flask_login import LoginManager, login_user, logout_user, login_required
from plenario.database import session as db_session
from plenario.models import User
from flask_wtf import Form
from flask_wtf import CsrfProtect
from wtforms import TextField, PasswordField
from wtforms.validators import DataRequired, Email
from functools import wraps
import json

auth = Blueprint('auth', __name__)
login_manager = LoginManager()
csrf = CsrfProtect()



def check_admin_status():
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            api_key = None
            resp = {
                'status': 'ok',
                'message': ''
            }
            status_code = 200
            if flask_session.get('user_id'):
                api_key = flask_session['user_id']
            elif request.form.get('api_key'):
                api_key = request.form['api_key']
            elif request.args.get('api_key'):
                api_key = request.args['api_key']
            else:
                try:
                    api_key = json.loads(request.data).get('api_key')
                except ValueError:
                    api_key = None
            if (api_key):
                user = db_session.query(User).get(api_key)
            flask_session['user_id'] = api_key
            return f(*args, **kwargs)
        return decorated
    return decorator


class LoginForm(Form):
    email = TextField('email', validators=[DataRequired(), Email()])
    password = PasswordField('password', validators=[DataRequired()])

    def __init__(self, *args, **kwargs):
        Form.__init__(self, *args, **kwargs)
        self.user = None

    def validate(self):
        rv = Form.validate(self)
        if not rv:
            return False

        user = db_session.query(User)\
            .filter(User.email == self.email.data).first()
        if user is None:
            self.email.errors.append('Email address is not registered')
            return False

        if not user.check_password(user.name, self.password.data):
            self.password.errors.append('Password is not valid')
            return False

        self.user = user
        return True

class AddUserForm(Form):
    name = TextField('name', validators=[DataRequired()])
    email = TextField('email', validators=[DataRequired(), Email()])
    password = PasswordField('password', validators=[DataRequired()])

    def __init__(self, *args, **kwargs):
        Form.__init__(self, *args, **kwargs)
        self.user = None

    def validate(self):
        rv = Form.validate(self)
        if not rv:
            return False

        existing_name = db_session.query(User)\
            .filter(User.name == self.name.data).first()
        if existing_name:
            self.name.errors.append('Name is already registered')
            return False

        existing_email = db_session.query(User)\
            .filter(User.email == self.email.data).first()
        if existing_email:
            self.email.errors.append('Email address is already registered')
            return False
        
        return True

class ResetPasswordForm(Form):
    old_password = PasswordField('old_password', validators=[DataRequired()])
    new_password = PasswordField('new_password', validators=[DataRequired()])

@login_manager.user_loader
def load_user(userid):
    return db_session.query(User).get(userid)

@auth.route('/logout/')
def logout():
    logout_user()
    return redirect(url_for('views.index'))

@auth.route('/login/', methods=['GET', 'POST'])
def login():
    form = LoginForm()
    if form.validate_on_submit():
        user = form.user
        login_user(user)
        return redirect(request.args.get('next') or url_for('views.index'))
    email = form.email.data
    return render_template('admin/login.html', form=form, email=email)

@auth.route('/admin/add-user/', methods=['GET', 'POST'])
@login_required
def add_user():
    form = AddUserForm()
    if form.validate_on_submit():
        user_info = {
            'name': form.name.data,
            'email': form.email.data,
            'password': form.password.data
        }
        user = User(**user_info)
        db_session.add(user)
        db_session.commit()
    context = {
        'form': form,
        'name': form.name.data,
        'email': form.email.data,
        'users': db_session.query(User).all()
    }
    return render_template('admin/add-user.html', **context)

@auth.route('/admin/reset-password/', methods=['GET', 'POST'])
@login_required
def reset_password():
    form = ResetPasswordForm()
    errors = []
    if form.validate_on_submit():
        user = db_session.query(User).get(flask_session['user_id'])
        check = user.check_password(user.name, form.old_password.data)
        if check:
            user.password = form.new_password.data
            db_session.add(user)
            db_session.commit()
            flash('Password reset successful!', 'success')
        else:
            errors.append('Password is not correct')
    return render_template('admin/reset-password.html', form=form, errors=errors)
