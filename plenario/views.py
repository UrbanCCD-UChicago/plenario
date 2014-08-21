from flask import make_response, request, render_template, current_app, g, \
    Blueprint, flash
from plenario.models import MasterTable, MetaTable
from plenario.database import session
from plenario.utils.helpers import get_socrata_data_info
from plenario.tasks import update_dataset as update_dataset_task
from flask_login import login_required
from datetime import datetime, timedelta
from urlparse import urlparse
import requests
from flask_wtf import Form
from wtforms import TextField, PasswordField, DateField, SelectField
from wtforms.validators import DataRequired, Email
from dateutil import parser
import json

views = Blueprint('views', __name__)

@views.route('/')
def index():
    return render_template('index.html')

@views.route('/explore')
def explore_view():
    return render_template('explore.html')

@views.route('/api-docs')
def api_docs_view():
    return render_template('api-docs.html')

@views.route('/about')
def about_view():
    return render_template('about.html')

@views.route('/add-dataset', methods=['GET', 'POST'])
@login_required
def add_dataset():
    dataset_info = {}
    errors = []
    if request.method == 'POST':
        url = request.form.get('dataset_url')
        if url:
            parsed = urlparse(url)
            host = 'https://%s' % parsed.netloc
            path = 'api/views'
            fourbyfour = parsed.path.split('/')[-1]
            view_url = '%s/%s/%s' % (host, path, fourbyfour)
            dataset_info, errors, status_code = get_socrata_data_info(view_url)
            if status_code is not None and status_code != 200:
                errors.append('URL returns a %s status code' % status_code)
            dataset_info['submitted_url'] = url
        else:
            errors.append('Need a URL')
    context = {'dataset_info': dataset_info, 'errors': errors}
    return render_template('add-dataset.html', **context)

@views.route('/view-datasets')
@login_required
def view_datasets():
    datasets = session.query(MetaTable).all()
    return render_template('view-datasets.html', datasets=datasets)

class EditDatasetForm(Form):
    """ 
    Form to edit meta_master information for a dataset
    """
    human_name = TextField('human_name', validators=[DataRequired()])
    description = TextField('description', validators=[DataRequired()])
    obs_from = DateField('obs_from', validators=[DataRequired(message="Start of date range must be a valid date")])
    obs_to = DateField('obs_to', validators=[DataRequired(message="End of date range must be a valid date")])
    update_freq = SelectField('update_freq', 
                              choices=[('daily', 'Daily'),
                                       ('houly', 'Hourly'),
                                       ('weekly', 'Weekly'),
                                       ('monthly', 'Monthly')], 
                              validators=[DataRequired()])
    business_key = TextField('business_key', validators=[DataRequired()])
    observed_date = TextField('observed_date', validators=[DataRequired()])
    latitude = TextField('latitude')
    longitude = TextField('longitude')
    location = TextField('location')

    def validate(self):
        rv = Form.validate(self)
        if not rv:
            return False
        
        valid = True
        
        if not self.location.data and not self.latitude.data and not self.longitude.data:
            valid = False
            self.location.errors.append('You must either provide a Latitude and Longitude field name or a Location field name')
        
        if self.longitude.data and not self.latitude.data:
            valid = False
            self.latitude.errors.append('You must provide both a Latitude field name and a Longitude field name')
        
        if self.latitude.data and not self.longitude.data:
            valid = False
            self.longitude.errors.append('You must provide both a Latitude field name and a Longitude field name')

        return valid

@views.route('/edit-dataset/<four_by_four>', methods=['GET', 'POST'])
@login_required
def edit_dataset(four_by_four):
    form = EditDatasetForm()
    meta = session.query(MetaTable).get(four_by_four)
    view_url = 'http://%s/api/views/%s' % (urlparse(meta.source_url).netloc, four_by_four)
    socrata_info, errors, status_code = get_socrata_data_info(view_url)
    if form.validate_on_submit():
        upd = {
            'human_name': form.human_name.data,
            'description': form.description.data,
            'obs_from': form.obs_from.data,
            'obs_to': form.obs_to.data,
            'update_freq': form.update_freq.data,
            'business_key': form.business_key.data,
            'latitude': form.latitude.data,
            'longitude': form.longitude.data,
            'location': form.location.data,
            'observed_date': form.observed_date.data,
        }
        session.query(MetaTable)\
            .filter(MetaTable.four_by_four == four_by_four)\
            .update(upd)
        session.commit()
        flash('%s updated successfully!' % meta.human_name)
    context = {
        'form': form,
        'meta': meta,
        'socrata_info': socrata_info
    }
    return render_template('edit-dataset.html', **context)

@views.route('/update-dataset/<four_by_four>')
def update_dataset(four_by_four):
    update_dataset_task.delay(four_by_four)
    return make_response(json.dumps({'status': 'success'}))

@views.route('/license')
def license_view():
    return render_template('license.html')