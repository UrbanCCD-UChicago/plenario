from flask import make_response, request, render_template, current_app, g, \
    Blueprint, flash
from plenario.models import MasterTable, MetaTable
from plenario.database import session
from plenario.utils.helpers import get_socrata_data_info, iter_column
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
from cStringIO import StringIO
from csvkit.unicsv import UnicodeCSVReader

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
            r = requests.get(url, stream=True)
            dataset_info['name'] = urlparse(url).path.split('/')[-1]
            inp = StringIO()
            line_no = 0
            lines = []
            for line in r.iter_lines():
                try:
                    inp.write(line + '\n')
                    line_no += 1
                    if line_no > 1000:
                        raise StopIteration
                except StopIteration:
                    break
            inp.seek(0)
            reader = UnicodeCSVReader(inp)
            header = reader.next()
            col_types = []
            for col in range(len(header)):
                col_types.append(iter_column(col, inp))
            dataset_info['columns'] = []
            for idx, col in enumerate(col_types):
                d = {
                    'human_name': header[idx],
                    'data_type': col.__visit_name__.lower()
                }
                dataset_info['columns'].append(d)
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
    attribution = TextField('attribution', validators=[DataRequired()])
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

@views.route('/edit-dataset/<source_url_hash>', methods=['GET', 'POST'])
@login_required
def edit_dataset(source_url_hash):
    form = EditDatasetForm()
    meta = session.query(MetaTable).get(source_url_hash)
    parsed_url = urlparse(meta.source_url)
    four_by_four = parsed_url.path.split('/')[-1]
    view_url = 'http://%s/api/views/%s' % (parsed_url.netloc, four_by_four)
    socrata_info, errors, status_code = get_socrata_data_info(view_url)
    if form.validate_on_submit():
        upd = {
            'human_name': form.human_name.data,
            'description': form.description.data,
            'attribution': form.attribution.data,
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
            .filter(MetaTable.source_url_hash == meta.source_url_hash)\
            .update(upd)
        session.commit()
        flash('%s updated successfully!' % meta.human_name, 'success')
    context = {
        'form': form,
        'meta': meta,
        'socrata_info': socrata_info
    }
    return render_template('edit-dataset.html', **context)

@views.route('/update-dataset/<source_url_hash>')
def update_dataset(source_url_hash):
    result = update_dataset_task.delay(source_url_hash)
    return make_response(json.dumps({'status': 'success', 'task_id': result.id}))

@views.route('/check-update/<task_id>')
def check_update(task_id):
    result = update_dataset_task.AsyncResult(task_id)
    if result.ready():
        r = {'status': 'ready'}
    else:
        r = {'status': 'pending'}
    resp = make_response(json.dumps(r))
    resp.headers['Content-Type'] = 'application/json'
    return resp


@views.route('/license')
def license_view():
    return render_template('license.html')


@views.route('/terms')
def terms_view():
    return render_template('terms.html')
