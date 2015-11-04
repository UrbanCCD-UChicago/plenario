from flask import make_response, request, redirect, url_for, render_template, current_app, g, \
    Blueprint, flash, session as flask_session
from plenario.models import MasterTable, MetaTable, User, ShapeMetadata
from plenario.database import session, Base, app_engine as engine
from plenario.utils.helpers import get_socrata_data_info, iter_column, send_mail, slugify
from plenario.tasks import update_dataset as update_dataset_task, \
    delete_dataset as delete_dataset_task, add_dataset as add_dataset_task, \
    add_shape as add_shape_task, delete_shape as delete_shape_task
from flask_login import login_required
from datetime import datetime, timedelta
from urlparse import urlparse
import requests
from flask_wtf import Form
from wtforms import TextField, PasswordField, DateField, SelectField
from wtforms.validators import DataRequired, Email
from dateutil import parser
import json
import re
from cStringIO import StringIO
from csvkit.unicsv import UnicodeCSVReader
from sqlalchemy import Table, select, func, text, and_
from plenario.settings import CACHE_CONFIG
import string
import sqlalchemy
from hashlib import md5
import traceback
from sqlalchemy.exc import NoSuchTableError

views = Blueprint('views', __name__)

@views.route('/')
def index():
    return render_template('index.html')

@views.route('/explore')
def explore_view():
    return render_template('explore.html')

@views.route('/api-docs')
def api_docs_view():
    dt_now = datetime.now()
    
    return render_template('api-docs.html', yesterday=dt_now-timedelta(days=1))

@views.route('/about')
def about_view():
    return render_template('about.html')

@views.route('/examples')
def examples_view():
    return render_template('examples.html')

@views.route('/maintenance')
def maintenance():
    return render_template('maintenance.html'), 503

# Given a URL, this function returns a tuple (dataset_info, errors, socrata_source)
def get_context_for_new_dataset(url):
    dataset_info = {}
    errors = []
    socrata_source = False
    if url:
        url = url.strip(' \t\n\r') # strip whitespace, tabs, etc
        four_by_four = re.findall(r'/([a-z0-9]{4}-[a-z0-9]{4})', url)
        errors = True
        if four_by_four:
            parsed = urlparse(url)
            host = 'https://%s' % parsed.netloc
            path = 'api/views'

            dataset_info, errors, status_code = get_socrata_data_info(host, path, four_by_four[-1])
            if not errors:
                socrata_source = True
                dataset_info['submitted_url'] = url
        if errors:
            errors = []
            try:
                r = requests.get(url, stream=True)
                status_code = r.status_code
            except requests.exceptions.InvalidURL:
                errors.append('Invalid URL')
            except requests.exceptions.ConnectionError:
                errors.append('URL can not be reached')
            if status_code != 200:
                errors.append('URL returns a %s status code' % status_code)
            if not errors:
                dataset_info['submitted_url'] = url
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
                inp.seek(0)
                for col in range(len(header)):
                    col_types.append(iter_column(col, inp)[0])
                dataset_info['columns'] = []
                for idx, col in enumerate(col_types):
                    d = {
                        'human_name': header[idx],
                        'data_type': col.__visit_name__.lower()
                    }
                    dataset_info['columns'].append(d)
    else:
        errors.append('Need a URL')
    #print "get_context_for_new_dataset(): returning ", dataset_info, errors, socrata_source
    return (dataset_info, errors, socrata_source)

def table_row_estimate(table_name):
    try:
        q = text(''' 
            SELECT reltuples::bigint AS estimate FROM pg_class where relname=:table_name;
        ''')
        with engine.begin() as c:
            return list(c.execute(q, table_name=table_name))[0][0]
    except NoSuchTableError, e:
        print "Table %s doesn't exist" % table_name 

def add_dataset_to_metatable(request, url, dataset_id, dataset_info, socrata_source, approved_status):
    data_types = []
    business_key = None
    observed_date = None
    latitude = None
    longitude = None 
    location = None
    for k, v in request.form.iteritems():
        if k.startswith('data_type_'):
            key = k.replace("data_type_", "")
            data_types.append({"field_name": key, "data_type": v})

        if k.startswith('key_type_'):
            key = k.replace("key_type_", "")
            if (v == "business_key"): business_key = key
            if (v == "observed_date"): observed_date = key
            if (v == "latitude"): latitude = key
            if (v == "longitude"): longitude = key
            if (v == "location"): location = key

    if socrata_source:
        data_types = dataset_info['columns']
        url = dataset_info['source_url']

    d = {
        'dataset_name': slugify(request.form.get('dataset_name'), delim=u'_')[:50],
        'human_name': request.form.get('dataset_name'),
        'attribution': request.form.get('dataset_attribution'),
        'description': request.form.get('dataset_description'),
        'source_url': url,
        'source_url_hash': dataset_id,
        'update_freq': request.form.get('update_frequency'),
        'business_key': business_key,
        'observed_date': observed_date,
        'latitude': latitude,
        'longitude': longitude,
        'location': location,
        'contributor_name': request.form.get('contributor_name'),
        'contributor_organization': request.form.get('contributor_organization'),
        'contributor_email': request.form.get('contributor_email'),
        'contributed_data_types': json.dumps(data_types),
        'approved_status': approved_status,
        'is_socrata_source': socrata_source
    }

    # add this to meta_master
    md = MetaTable(**d)
    session.add(md)
    session.commit()

    return md

def approve_dataset(source_url_hash):
    # get the MetaTable row and change the approved_status and bounce back to view-datasets.

    meta = session.query(MetaTable).get(source_url_hash)

    json_data_types = None
    if ((not meta.is_socrata_source) and meta.contributed_data_types):
        json_data_types = json.loads(meta.contributed_data_types)
        
    add_dataset_task.delay(source_url_hash, data_types=json_data_types)
    
    upd = { 'approved_status': 'true' }

    meta.approved_status = 'true'
    session.commit()

    # Email the user who submitted that their dataset has been approved.
    # email the response to somebody

    msg_body = """Hello %s,\r\n
\r\n
Your dataset has been approved and added to Plenar.io:\r\n
\r\n
%s\r\n
\r\n
It should appear on http://plenar.io within 24 hours.\r\n
\r\n
Thank you!\r\n
The Plenario Team\r\n
http://plenar.io""" % (meta.contributor_name, meta.human_name)

    send_mail(subject="Your dataset has been added to Plenar.io", 
        recipient=meta.contributor_email, body=msg_body)


# /contribute is similar to /admin/add-dataset, but sends an email instead of actually adding
@views.route('/contribute', methods=['GET','POST'])
def contrib_view():
    dataset_info = {}
    errors = []
    socrata_source = False

    url = ""
    dataset_id = None
    md = None

    if request.args.get('dataset_url'):
        url = request.args.get('dataset_url')
        (dataset_info, errors, socrata_source) = get_context_for_new_dataset(url)

        # check if dataset with the same URL has already been loaded
        dataset_id = md5(url).hexdigest()
        md = session.query(MetaTable).get(dataset_id)
        if md:
            errors.append("A dataset with that URL has already been loaded: '%s'" % md.human_name)

    if request.method == 'POST' and not md:
        md = add_dataset_to_metatable(request, url, dataset_id, dataset_info, socrata_source, approved_status=False)

        # email a confirmation to the submitter
        msg_body = """Hello %s,\r\n\r\n
We received your recent dataset submission to Plenar.io:\r\n\r\n%s\r\n\r\n
After we review it, we'll notify you when your data is loaded and available.\r\n\r\n
Thank you!\r\nThe Plenario Team\r\nhttp://plenar.io""" % (request.form.get('contributor_name'), md.human_name)

        send_mail(subject="Your dataset has been submitted to Plenar.io", 
            recipient=request.form.get('contributor_email'), body=msg_body)

        return redirect(url_for('views.contrib_thankyou'))

    context = {'dataset_info': dataset_info, 'form': request.form, 'errors': errors, 'socrata_source': socrata_source}
    return render_template('submit-table.html', **context)

@views.route('/contribute-thankyou')
def contrib_thankyou():
    context = {}
    return render_template('contribute_thankyou.html', **context)


@views.route('/admin/add-dataset/table', methods=['GET', 'POST'])
@login_required
def add_table():
    dataset_info = {}
    errors = []
    socrata_source = False

    url = ""
    dataset_id = None
    md = None

    if request.args.get('dataset_url'):
        url = request.args.get('dataset_url')
        (dataset_info, errors, socrata_source) = get_context_for_new_dataset(url)

        # populate contributor info from session
        user = session.query(User).get(flask_session['user_id'])
        dataset_info['contributor_name'] = user.name
        dataset_info['contributor_organization'] = 'Plenario Admin'
        dataset_info['contributor_email'] = user.email

        # check if dataset with the same URL has already been loaded
        dataset_id = md5(url).hexdigest()
        md = session.query(MetaTable).get(dataset_id)
        if md:
            errors.append("A dataset with that URL has already been loaded: '%s'" % md.human_name)

    if request.method == 'POST' and not md:
        md = add_dataset_to_metatable(request, url, dataset_id, dataset_info, socrata_source, approved_status=True)
        
        json_data_types = None
        if ((not md.is_socrata_source) and md.contributed_data_types):
            json_data_types = json.loads(md.contributed_data_types)

        add_dataset_task.delay(md.source_url_hash, data_types=json_data_types)
        
        flash('%s added successfully!' % md.human_name, 'success')
        return redirect(url_for('views.view_datasets'))
        
    context = {'dataset_info': dataset_info, 'errors': errors, 'socrata_source': socrata_source, 'is_admin': True}
    return render_template('submit-table.html', **context)


@views.route('/admin/add-shape', methods=['GET', 'POST'])
@login_required
def add_shape():

    errors = []

    if request.method == 'POST':
        try:
            human_name = request.form['dataset_name']
            source_url = request.form['source_url']
        except KeyError:
            # Front-end validation failed or someone is posting bogus query strings directly.
            # re-render with error message
            errors.append('A required field was not submitted.')

        # Does a shape dataset with this human_name already exist?
        if ShapeMetadata.get_by_human_name(human_name=human_name, caller_session=session):
            errors.append('A shape dataset with that name already exists.')

        if not errors:
            # Add the metadata right away
            meta = ShapeMetadata.add(caller_session=session, human_name=human_name, source_url=source_url)
            session.commit()

            # And tell a worker to go ingest it
            add_shape_task.delay(table_name=meta.dataset_name)

            flash('Plenario is now trying to ingest your shapefile.', 'success')
            return redirect(url_for('views.view_datasets'))

    return render_template('submit-shape.html', is_admin=True, errors=errors)

@views.route('/admin/view-datasets')
@login_required
def view_datasets():
    datasets_pending = session.query(MetaTable)\
        .filter(MetaTable.approved_status != 'true')\
        .all()

    counts = {
        'master_row_count': table_row_estimate('dat_master'),
        'weather_daily_row_count': table_row_estimate('dat_weather_observations_daily'),
        'weather_hourly_row_count': table_row_estimate('dat_weather_observations_hourly'),
        'census_block_row_count': table_row_estimate('census_blocks'),
    }

    try:
        q = text(''' 
            SELECT m.*, c.status, c.task_id
            FROM meta_master AS m 
            LEFT JOIN celery_taskmeta AS c 
              ON c.id = (
                SELECT id FROM celery_taskmeta 
                WHERE task_id = ANY(m.result_ids) 
                ORDER BY date_done DESC 
                LIMIT 1
              )
            WHERE m.approved_status = 'true'
        ''')
        with engine.begin() as c:
            datasets = list(c.execute(q))
    except NoSuchTableError, e:
        datasets = session.query(MetaTable)\
        .filter(MetaTable.approved_status == 'true')\
        .all()

    try:
        shape_datasets = ShapeMetadata.get_all_with_etl_status(caller_session=session)
    except NoSuchTableError:
        # If we can't find shape metadata, soldier on.
        shape_datasets = None

    return render_template('admin/view-datasets.html',
                           datasets_pending=datasets_pending,
                           datasets=datasets,
                           counts=counts,
                           shape_datasets=shape_datasets)


@views.route('/admin/shape-status/')
@login_required
def shape_status():
    table_name = request.args['table_name']
    shape_meta = ShapeMetadata.get_metadata_with_etl_result(table_name=table_name, caller_session=session)
    return render_template('admin/shape-status.html', shape=shape_meta)


@views.route('/admin/dataset-status/')
@login_required
def dataset_status():

    source_url_hash = request.args.get("source_url_hash")
    celery_table = Table('celery_taskmeta', Base.metadata, 
                         autoload=True, autoload_with=engine)
    results = []
    q = ''' 
        SELECT 
          m.human_name, 
          m.source_url_hash,
          c.status, 
          c.date_done,
          c.traceback,
          c.task_id
        FROM meta_master AS m, 
        UNNEST(m.result_ids) AS ids 
        LEFT JOIN celery_taskmeta AS c 
          ON c.task_id = ids
        WHERE c.date_done IS NOT NULL 
    '''

    if source_url_hash:
        q = q + "AND m.source_url_hash = :source_url_hash"

    q = q + " ORDER BY c.id DESC"

    with engine.begin() as c:
        results = list(c.execute(text(q), source_url_hash=source_url_hash))
    r = []
    for result in results:
        tb = None
        if result.traceback:
            tb = result.traceback\
                .replace('\r\n', '<br />')\
                .replace('\n\r', '<br />')\
                .replace('\n', '<br />')\
                .replace('\r', '<br />')
        d = {
            'human_name': result.human_name,
            'source_url_hash': result.source_url_hash,
            'status': result.status,
            'task_id': result.task_id,
            'traceback': tb,
            'date_done': None,
        }
        if result.date_done:
            d['date_done'] = result.date_done.strftime('%B %d, %Y %H:%M'),
        r.append(d)
    return render_template('admin/dataset-status.html', results=r)

class EditDatasetForm(Form):
    """ 
    Form to edit meta_master information for a dataset
    """
    human_name = TextField('human_name', validators=[DataRequired()])
    description = TextField('description', validators=[DataRequired()])
    attribution = TextField('attribution', validators=[DataRequired()])
    #obs_from = DateField('obs_from', validators=[DataRequired(message="Start of date range must be a valid date")])
    #obs_to = DateField('obs_to', validators=[DataRequired(message="End of date range must be a valid date")])
    update_freq = SelectField('update_freq', 
                              choices=[('daily', 'Daily'),
                                       ('weekly', 'Weekly'),
                                       ('monthly', 'Monthly'),
                                       ('yearly', 'Yearly')], 
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

@views.route('/admin/approve-dataset/<source_url_hash>', methods=['GET', 'POST'])
@login_required
def approve_dataset_view(source_url_hash):
    
    approve_dataset(source_url_hash)
    
    return redirect(url_for('views.view_datasets'))


@views.route('/admin/edit-dataset/<source_url_hash>', methods=['GET', 'POST'])
@login_required
def edit_dataset(source_url_hash):
    form = EditDatasetForm()
    meta = session.query(MetaTable).get(source_url_hash)

    fieldnames = None
    num_rows = 0
    num_weather_observations = 0
    num_rows_w_censusblocks = 0
    
    if (meta.approved_status == 'true'):
        try:
            table_name = 'dat_%s' % meta.dataset_name
            
            table = Table(table_name, Base.metadata,
                          autoload=True, autoload_with=engine)
            fieldnames = table.columns.keys()
            pk_name  =[p.name for p in table.primary_key][0]
            pk = table.c[pk_name]
            num_rows = session.query(pk).count()

            dat_master = Table('dat_master', Base.metadata, autoload=True, autoload_with=engine)

            sel = session.query(func.count(dat_master.c.master_row_id)).filter(and_(dat_master.c.dataset_name==meta.dataset_name,
                                                                                    dat_master.c.dataset_row_id==pk,
                                                                                    dat_master.c.weather_observation_id.isnot(None)))

            num_weather_observations = sel.first()[0]

            sel = session.query(func.count(dat_master.c.master_row_id)).filter(and_(dat_master.c.dataset_name==meta.dataset_name,
                                                                                    dat_master.c.dataset_row_id==pk,
                                                                                    dat_master.c.census_block.isnot(None)))

            num_rows_w_censusblocks = sel.first()[0]

            
        except sqlalchemy.exc.NoSuchTableError, e:
            # dataset has been approved, but perhaps still processing.
            pass

    if (not fieldnames):
        # This is the case when the csv dataset was not from Socrata
        fieldnames = []
        if meta.contributed_data_types:
            fieldnames = [f['field_name'] for f in json.loads(meta.contributed_data_types)]

    if form.validate_on_submit():
        upd = {
            'human_name': form.human_name.data,
            'description': form.description.data,
            'attribution': form.attribution.data,
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

        
        if (meta.approved_status != 'true'):
            approve_dataset(source_url_hash)
        
        flash('%s updated successfully!' % meta.human_name, 'success')
        return redirect(url_for('views.view_datasets'))
    else:
        pass

    context = {
        'form': form,
        'meta': meta,
        'fieldnames': fieldnames,
        'num_rows': num_rows,
        'num_weather_observations': num_weather_observations,
        'num_rows_w_censusblocks': num_rows_w_censusblocks
    }
    return render_template('admin/edit-dataset.html', **context)


@views.route('/admin/delete-shape/<table_name>')
@login_required
def delete_shape(table_name):
    result = delete_shape_task.delay(table_name)
    return make_response(json.dumps({'status': 'success', 'task_id': result.id}))


@views.route('/admin/delete-dataset/<source_url_hash>')
@login_required
def delete_dataset(source_url_hash):
    result = delete_dataset_task.delay(source_url_hash)
    return make_response(json.dumps({'status': 'success', 'task_id': result.id}))

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

@views.route('/terms')
def terms_view():
    return render_template('terms.html')
