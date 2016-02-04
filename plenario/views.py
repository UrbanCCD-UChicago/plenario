from flask import make_response, request, redirect, url_for, render_template, \
    Blueprint, flash, session as flask_session
from plenario.models import MetaTable, User, ShapeMetadata
from plenario.database import session, Base, app_engine as engine
from plenario.utils.helpers import send_mail, slugify, infer_csv_columns
from plenario.tasks import update_dataset as update_dataset_task, \
    delete_dataset as delete_dataset_task, add_dataset as add_dataset_task, \
    add_shape as add_shape_task, delete_shape as delete_shape_task
from flask_login import login_required
from datetime import datetime, timedelta
from urlparse import urlparse
import requests
from flask_wtf import Form
from wtforms import TextField, SelectField
from wtforms.validators import DataRequired
import json
import re
from cStringIO import StringIO
from sqlalchemy import Table, text
import sqlalchemy
from hashlib import md5
from sqlalchemy.exc import NoSuchTableError
from collections import namedtuple
import itertools

views = Blueprint('views', __name__)

'''(Mostly) Static pages'''

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


@views.route('/terms')
def terms_view():
    return render_template('terms.html')

'''Approve a dataset'''


# TODO: Catch an email exception and flash email failure


@views.route('/admin/approve-dataset/<source_url_hash>', methods=['GET', 'POST'])
@login_required
def approve_dataset_view(source_url_hash):
    approve_dataset(source_url_hash)
    return redirect(url_for('views.view_datasets'))


def approve_dataset(source_url_hash):
    # Approve it
    meta = session.query(MetaTable).get(source_url_hash)
    meta.approved_status = 'true'
    session.commit()
    # Ingest it
    add_dataset_task.delay(source_url_hash)

    # Email the submitter
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

#
''' Submit a dataset (Add it to MetaData
    and try to ingest it now or later.) '''
#


@views.route('/admin/add', methods=['GET', 'POST'])
@login_required
def admin_add_dataset():
    user = session.query(User).get(flask_session['user_id'])
    context = {'is_admin': True,
               'contributor_name': user.name,
               'contributor_organization': 'Plenario Admin',
               'contributor_email': user.email}
    return add(context)


@views.route('/add', methods=['GET', 'POST'])
def user_add_dataset():
    context = {'is_admin': False}
    return add(context)


def send_submission_email(dataset_name, contributor_name, contributor_email):
    msg_body = """Hello %s,\r\n\r\n
We received your recent dataset submission to Plenar.io:\r\n\r\n%s\r\n\r\n
After we review it, we'll notify you when your data is loaded and available.\r\n\r\n
Thank you!\r\nThe Plenario Team\r\nhttp://plenar.io""" % (contributor_name, dataset_name)

    send_mail(subject="Your dataset has been submitted to Plenar.io",
              recipient=contributor_email,
              body=msg_body)

    return redirect(url_for('views.contrib_thankyou'))


def render_with_context(context):
    return render_template('submit-table.html', **context)


def add(context):
    context['is_shapefile'] = request.args.get('is_shapefile')

    # Step 1: User looking at page for the first time
    if request.method == 'GET' and not request.args.get('dataset_url'):
        return render_with_context(context)

    # Step 2: User suggested a URL for a dataset
    if request.method == 'GET':
        return suggest(context)

    # Step 3: User POSTed all the submission data
    #          and made it past frontend validation
    if request.method == 'POST':
        return submit(context)


def submit(context):
    form = request.form
    is_shapefile = context['is_shapefile']
    is_admin = context['is_admin']

    try:  # Store the metadata
        human_name = form['dataset_name']
        if is_shapefile:
            if shape_already_submitted(human_name):
                msg = 'A Shapefile with this name has already been submitted'
                raise RuntimeError(msg)
            else:
                meta = shape_meta_from_submit_form(form, is_approved=is_admin)
        else:
            meta = point_meta_from_submit_form(form, is_approved=is_admin)
    except RuntimeError as e:
        context['error_msg'] = e.message
        return render_with_context(context)
    else:
        # Successfully stored the metadata
        # Now fire ingestion task...
        if is_admin:
            if is_shapefile:
                add_shape_task.delay(meta.dataset_name)
            else:
                add_dataset_task.delay(meta.source_url_hash)
        # or send thankyou email
        else:
            return send_submission_email(meta.human_name,
                                         meta.contributor_name,
                                         meta.contributor_email)


def suggest(context):
    is_shapefile = context['is_shapefile']
    is_csv = not is_shapefile
    suggested_url = request.args.get('dataset_url')
    try:
        suggestion = process_suggestion(suggested_url, is_shapefile)
        if is_csv and csv_already_submitted(suggestion.file_url):
            msg = 'A CSV at this url has already been submitted'
            raise RuntimeError(msg)
    except RuntimeError as e:
        # Something was jank with that suggestion.
        context['error_msg'] = e.message
        return render_with_context(context)
    else:
        # Merge exsting context into new context so that no values
        # from the suggestion overwrite existing values
        suggestion_context = context_from_suggestion(suggestion)
        suggestion_context.update(context)
        return render_with_context(suggestion_context)

''' Submission helpers '''


def form_columns(form):
    """
    :param form: Taken from requests.form
    :return: columns: list of slugified column names
             labels: dict mapping string labels of special column types
             (observed_date, latitude, longitude, location)
             to names of columns
    """

    labels = {}
    columns = []
    for k, v in form.iteritems():
        if k.startswith('col_name_'):
            # key_type_observed_date
            key = k.replace("col_name_", "")
            columns.append(key)
            # e.g labels['observed_date'] = 'date'
            labels[v] = key
    return columns, labels


def csv_already_submitted(url):
    hash = md5(url).hexdigest()
    return session.query(MetaTable).get(hash) is not None


def shape_already_submitted(name):
    return session.query(ShapeMetadata).get(name) is not None


@views.route('/contribute-thankyou')
def contrib_thankyou():
    context = {}
    return render_template('contribute_thankyou.html', **context)


''' Helpers that can create a new MetaTable instance
    with a filled out submission form and dataset_info
    taken from the source URL.'''


def point_meta_from_submit_form(form, is_approved):
    column_names, labels = form_columns(form)
    name = slugify(form['dataset_name'], delim=u'_')[:50]

    md = MetaTable(
        url=form['dataset_url'],
        view_url=form.get('view_url'),
        dataset_name=name,
        human_name=form['dataset_name'],
        attribution=form.get('dataset_attribution'),
        description=form.get('dataset_description'),
        update_freq=form['update_frequency'],
        contributor_name=form['contributor_name'],
        contributor_organization=form.get('contributor_organization'),
        contributor_email=form['contributor_email'],
        approved_status=is_approved,
        observed_date=labels['observed_date'],
        latitude=labels.get('latitude', None),
        longitude=labels.get('longitude', None),
        location=labels.get('location', None),
        column_names=column_names
    )
    session.add(md)
    session.commit()
    return md


def shape_meta_from_submit_form(form, is_approved):
    md = ShapeMetadata.add(
        human_name=form['dataset_name'],
        source_url=form['source_url'],
        view_url=form.get('view_url'),
        attribution=form.get('dataset_attribution'),
        description=form.get('dataset_description'),
        update_freq=form['update_frequency'],
        contributor_name=form['contributor_name'],
        contributor_organization=form.get('contributor_organization'),
        contributor_email=form['contributor_email'],
        approved_status=is_approved)
    session.commit()
    return md



ColumnMeta = namedtuple('ColumnMeta', 'name type_')
DescriptionMeta = namedtuple("DescriptionMeta",
                             'human_name attribution description')
ContributorMeta = namedtuple('ContributorMeta', 'name organization email')


'''Suggestion helpers.'''


def _assert_reachable(url):
    try:
        resp = requests.head(url)
        assert resp.status_code != 404
    except:
        raise RuntimeError('Could not reach URL ' + url)


def is_certainly_html(url):
    head = requests.head(url)
    if head.status_code != 200:
        return False
    else:
        try:
            return 'text/html' in head.headers['content-type']
        except KeyError:
            # No content-type? Bummer.
            return False


def context_from_suggestion(suggestion):
    # Start with the attributes guaranteed to be present
    context = {
        'submitted_url': suggestion.submitted_url,
        'file_url': suggestion.file_url,
        'view_url': suggestion.view_url,
        'columns': suggestion.columns
    }
    # If this suggestion has column info (read: is for a CSV),
    # then put those tuples in dict form.
    if context['columns']:
        context['columns'] = [col._asdict() for col in context['columns']]

    # Get what metadata we can glean
    try:
        context.update(suggestion.description_meta._asdict())
    except AttributeError:
        context.update(DescriptionMeta(None, None, None)._asdict())

    return context


def process_suggestion(url, is_shapefile=False):
    _assert_reachable(url)
    if SocrataSuggestion.is_socrata_url(url):
        suggestion = SocrataSuggestion(url, is_shapefile)
    else:
        suggestion = GenericSuggestion(url, is_shapefile)
    return suggestion


class GenericSuggestion(object):
    def __init__(self, url, is_shapefile=False):
        if is_certainly_html(url):
            raise RuntimeError('URL must point directly to a CSV or Shapefile')
        self.file_url = url
        self.submitted_url = url
        self.view_url = None
        self.columns = (None if is_shapefile else self._infer_columns())

    def _infer_columns(self):
        r = requests.get(self.file_url, stream=True)
        inp = StringIO()

        head = itertools.islice(r.iter_lines(), 1000)
        for line in head:
            inp.write(line + '\n')
        inp.seek(0)
        column_info = infer_csv_columns(inp)

        return [ColumnMeta(name, type_) for name, type_, _ in column_info]


class SocrataSuggestion(object):
    """
    All the metadata we can derive from a Socrata 4x4.
    Includes attribution and description.
    Can also derive url of file
    given a url to a UI page describing that file.

    Throws an exception if either the given url or derived url
    does not point to a file of the right type.
    """

    def __init__(self, url, is_shapefile=False):
        self.four_by_four = self._extract_four_by_four(url)
        self._metadata = None
        self._is_shapefile = is_shapefile

        if self.four_by_four is None:
            msg = 'URLs to Socrata datasets must contain a 4x4 id'
            raise RuntimeError(msg)
        self.submitted_url = url

        self.description_meta = self.derive_description_meta()
        self.view_url, self.file_url = self._derive_urls()

        self.columns = (None if is_shapefile else self._derive_columns())

    def _derive_columns(self):
        return [ColumnMeta(c['name'], c['dataTypeName'])
                for c in self.metadata['columns']]

    def _derive_urls(self):
        view_url = self._derive_view_url()
        file_url = self._derive_file_url(view_url)
        return view_url, file_url

    def _derive_view_url(self):
        """
        Try the "standard" url formats that we can construct
        with the submitted URL.
        """
        # CSV case
        if not self._is_shapefile:
            return '{}/api/views/{}/rows'.format(self.url_prefix(),
                                                 self.four_by_four)
        # Shapefile case
        if is_certainly_html(self.submitted_url):
            # If the user pointed us to HTML, use that.
            return self.submitted_url
        else:
            # Don't know of a consistent way to derive
            # an HTML view of a Socrata shape dataset. :(
            return None

    def _derive_file_url(self, view_url):
        if self._is_shapefile:
            return self._shapefile_file_url()
        else:
            # Assumes view_url is of the format '{}/api/views/{}/rows'
            return '%s.csv?accessType=DOWNLOAD' % view_url

    def _shapefile_file_url(self):
        # I noticed that if Socrata displays the shape as a map,
        # we can usually download through the geospatial API.
        # When it doesn't display the map,
        # we can download with /application/zip
        # This heuristic tends to work,
        # but a more robust way might be drilling into
        # metadata['metadata'] and seeing if there's a 'geo' key there
        # that denotes the geospatial API is enabled.

        blob_url = '{}/download/{}/application/zip'\
            .format(self.url_prefix(), self.four_by_four)
        map_url = '{}/api/geospatial/{}?method=export&format=Shapefile'.\
            format(self.url_prefix(), self.four_by_four)
        try:
            display_type = self.metadata['displayType']
        except KeyError:
            # No display_type means it's definitely a blob.
            return blob_url
        else:
            # Or maybe we were told it's a blob.
            return blob_url if display_type == 'blob' else map_url

    def url_prefix(self):
        parsed = urlparse(self.submitted_url)
        if not parsed.scheme:
            raise RuntimeError('URL missing protocol (like https://)')

        prefix = parsed.scheme + '://' + parsed.netloc
        return prefix

    def derive_description_meta(self):
        # Grab from correct JSON fields
        description = self.metadata['description']
        human_name = self.metadata['name']
        attribution = self.metadata['attribution']

        return DescriptionMeta(description=description,
                               human_name=human_name,
                               attribution=attribution)

    @property
    def metadata(self):
        """
        Dictionary of metadata pulled straight from the Socrata API
        """
        if self._metadata:
            return self._metadata

        # Construct for the first time
        prefix = self.url_prefix()
        metadata_endpoint = '{}/api/views/{}'.format(prefix, self.four_by_four)
        resp = requests.get(metadata_endpoint)
        self._metadata = resp.json()
        return self._metadata

    @staticmethod
    def _extract_four_by_four(url):
        """
        Return last string fragment matching Socrata 4x4 pattern
        :param url: URL from which to extract Socrata 4x4 id.
        :type url: str
        :return: 4x4 string like abc1-d2ef if found
                 else None
        """
        url = url.strip(' \t\n\r')  # strip whitespace, tabs, etc
        matches = re.findall(r'/([a-z0-9]{4}-[a-z0-9]{4})', url)
        if matches:
            return matches[-1]
        else:
            return None

    @classmethod
    def is_socrata_url(cls, url):
        return cls._extract_four_by_four(url) is not None


''' Monitoring and editing point datasets '''


@views.route('/admin/view-datasets')
@login_required
def view_datasets():
    datasets_pending = session.query(MetaTable)\
        .filter(MetaTable.approved_status != True)\
        .all()

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
    except NoSuchTableError:
        datasets = session.query(MetaTable)\
            .filter(MetaTable.approved_status == True)\
            .all()

    try:
        shape_datasets = ShapeMetadata.get_all_with_etl_status()
    except NoSuchTableError:
        # If we can't find shape metadata, soldier on.
        shape_datasets = None

    return render_template('admin/view-datasets.html',
                           datasets_pending=datasets_pending,
                           datasets=datasets,
                           shape_datasets=shape_datasets)


@views.route('/admin/dataset-status/')
@login_required
def dataset_status():

    source_url_hash = request.args.get("source_url_hash")

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
        name = session.query(MetaTable).get(source_url_hash).dataset_name
        q = q + "AND m.source_url_hash = :source_url_hash"
    else:
        name = None

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
    return render_template('admin/dataset-status.html', results=r, name=name)


class EditDatasetForm(Form):
    """ 
    Form to edit meta_master information for a dataset
    """
    human_name = TextField('human_name', validators=[DataRequired()])
    description = TextField('description', validators=[DataRequired()])
    attribution = TextField('attribution', validators=[DataRequired()])
    update_freq = SelectField('update_freq', 
                              choices=[('daily', 'Daily'),
                                       ('weekly', 'Weekly'),
                                       ('monthly', 'Monthly'),
                                       ('yearly', 'Yearly')], 
                              validators=[DataRequired()])
    observed_date = TextField('observed_date', validators=[DataRequired()])
    latitude = TextField('latitude')
    longitude = TextField('longitude')
    location = TextField('location')

    def validate(self):
        rv = Form.validate(self)
        if not rv:
            return False
        
        valid = True
        
        if not self.location.data and (not self.latitude.data or not self.longitude.data):
            valid = False
            self.location.errors.append('You must either provide a Latitude and Longitude field name or a Location field name')
        
        if self.longitude.data and not self.latitude.data:
            valid = False
            self.latitude.errors.append('You must provide both a Latitude field name and a Longitude field name')
        
        if self.latitude.data and not self.longitude.data:
            valid = False
            self.longitude.errors.append('You must provide both a Latitude field name and a Longitude field name')

        return valid


@views.route('/admin/edit-dataset/<source_url_hash>', methods=['GET', 'POST'])
@login_required
def edit_dataset(source_url_hash):
    form = EditDatasetForm()
    meta = session.query(MetaTable).get(source_url_hash)
    fieldnames = meta.column_names
    num_rows = 0
    
    if meta.approved_status:
        try:
            table_name = meta.dataset_name
            table = Table(table_name, Base.metadata,
                          autoload=True, autoload_with=engine)

            # Would prefer to just get the names from the metadata
            # without needing to reflect.
            fieldnames = table.columns.keys()
            pk_name = [p.name for p in table.primary_key][0]
            pk = table.c[pk_name]
            num_rows = session.query(pk).count()
            
        except sqlalchemy.exc.NoSuchTableError:
            # dataset has been approved, but perhaps still processing.
            pass

    if form.validate_on_submit():
        upd = {
            'human_name': form.human_name.data,
            'description': form.description.data,
            'attribution': form.attribution.data,
            'update_freq': form.update_freq.data,
            'latitude': form.latitude.data,
            'longitude': form.longitude.data,
            'location': form.location.data,
            'observed_date': form.observed_date.data,
        }
        session.query(MetaTable)\
            .filter(MetaTable.source_url_hash == meta.source_url_hash)\
            .update(upd)
        session.commit()

        if not meta.approved_status:
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
    }
    return render_template('admin/edit-dataset.html', **context)


@views.route('/admin/delete-dataset/<source_url_hash>')
@login_required
def delete_dataset(source_url_hash):
    result = delete_dataset_task.delay(source_url_hash)
    return make_response(json.dumps({'status': 'success',
                                     'task_id': result.id}))


@views.route('/update-dataset/<source_url_hash>')
def update_dataset(source_url_hash):
    result = update_dataset_task.delay(source_url_hash)
    return make_response(json.dumps({'status': 'success',
                                     'task_id': result.id}))


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


''' Shape monitoring '''


@views.route('/admin/shape-status/')
@login_required
def shape_status():
    table_name = request.args['table_name']
    shape_meta = ShapeMetadata.get_metadata_with_etl_result(table_name)
    return render_template('admin/shape-status.html', shape=shape_meta)


@views.route('/admin/delete-shape/<table_name>')
@login_required
def delete_shape(table_name):
    result = delete_shape_task.delay(table_name)
    return make_response(json.dumps({'status': 'success',
                                     'task_id': result.id}))
