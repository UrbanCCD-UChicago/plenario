import itertools
import json
import re
import dateutil.relativedelta
from cStringIO import StringIO
from collections import namedtuple
from datetime import datetime
from hashlib import md5
from urlparse import urlparse

import requests
import sqlalchemy
from flask import make_response, request, redirect, url_for, render_template, \
    Blueprint, flash, session as flask_session
from flask_login import login_required
from flask_wtf import Form
from sqlalchemy import Table, text
from sqlalchemy.exc import NoSuchTableError
from wtforms import SelectField, StringField
from wtforms.validators import DataRequired

from plenario.api.jobs import submit_job, get_status, get_job, JobsQueue
from plenario.database import session, Base, app_engine as engine, fast_count
from plenario.models import MetaTable, User, ShapeMetadata, Workers
from plenario.models_.ETLTask import ETLType, add_task
from plenario.models_.ETLTask import fetch_pending_tables, fetch_table_etl_status
from plenario.utils.helpers import send_mail, slugify, infer_csv_columns

views = Blueprint('views', __name__)

'''(Mostly) Static pages'''


@views.route('/')
def index():
    return render_template('index.html')


@views.route('/explore')
def explore_view():
    return render_template('explore.html')


# If the user requests a nested URL within the Ember app,
# we still just need to render the Ember app.
@views.route('/explore/<path:path>')
def explore_kludge(path):
    return render_template('explore.html')


@views.route('/api-docs')
def api_docs_view():
    return redirect("http://docs.plenar.io", code=302)


@views.route('/about')
def about_view():
    return render_template('about.html')


@views.route('/examples')
def examples_view():
    return redirect('https://medium.com/plenario-dev', code=302)


@views.route('/maintenance')
def maintenance():
    return render_template('maintenance.html'), 503


@views.route('/terms')
def terms_view():
    return render_template('terms.html')


@views.route('/workers')
def workers():
    q = session.query(Workers).all()
    workerlist = [row.__dict__ for row in q]
    now = datetime.now()
    nominal = 0
    loaded = 0
    dead = 0
    for worker in workerlist:
        if worker["job"]:
            job = json.loads(get_job(worker["job"]).get_data())
            if job.get("error"):
                worker["status"] = "dead"
                worker["jobinfo"] = {
                    "status": "TIMED OUT",
                    "workers": "",
                    "queuetime": "",
                    "worktime": "",
                    "endpoint": ""
                }
            else:
                if job["status"]["meta"].get("lastStartTime"):
                    diff = dateutil.relativedelta.relativedelta(now,
                                                            datetime.strptime(job["status"]["meta"]["lastStartTime"],
                                                                    "%Y-%m-%d %H:%M:%S.%f"))
                else:
                    diff = dateutil.relativedelta.relativedelta(now,
                                                            datetime.strptime(job["status"]["meta"]["startTime"],
                                                                                  "%Y-%m-%d %H:%M:%S.%f"))
                worker["jobinfo"] = {
                    "status": job["status"]["status"],
                    "workers": ", ".join(job["status"]["meta"]["workers"]),
                    "queuetime": job["status"]["meta"]["queueTime"],
                    "worktime": " {}h {}m {}s".format(diff.hours, diff.minutes, diff.seconds).replace(
                        " 0h ", "  ").replace(" 0m ", " ")[1:],
                    "endpoint": job["request"]["endpoint"]
                }

        lastseen = (now - datetime.strptime(worker["timestamp"], "%Y-%m-%d %H:%M:%S.%f")).total_seconds()
        if lastseen > 1200 or worker.get("status"):
            worker["status"] = "dead"
            dead += 1
        elif lastseen > 300:
            worker["status"] = "overload"
            loaded += 1
        elif lastseen > 60:
            worker["status"] = "load"
            loaded += 1
        else:
            worker["status"] = "nominal"
            nominal += 1

        diff = dateutil.relativedelta.relativedelta(now, datetime.fromtimestamp(worker["uptime"]))
        worker["humanized_uptime"] = " {}d {}h {}m {}s".format(
            diff.days, diff.hours, diff.minutes, diff.seconds).replace(
            " 0d ", "  ").replace(" 0h ", " ").replace(" 0m ", " ")[1:]
        diff = dateutil.relativedelta.relativedelta(datetime.fromtimestamp(lastseen),
                                                    datetime.fromtimestamp(0))
        worker["lastseen"] = " {}d {}h {}m {}s ago".format(
            diff.days, diff.hours, diff.minutes, diff.seconds).replace(
            " 0d ", "  ").replace(" 0h ", " ").replace(" 0m ", " ")[1:]

    workerlist.sort(key=lambda worker: worker["name"])
    jobs = JobsQueue.count()
    workercounts = {
        "total": len(workerlist),
        "nominal": nominal,
        "loaded": loaded,
        "dead": dead
    }
    return render_template('workers.html', workers=workerlist,
                           workercounts=workercounts, queuelength=jobs, overload=(jobs>=8))

@views.route('/workers/purge')
def purge_workers():
    Workers.purge()
    return redirect(url_for('views.workers'))

'''Approve a dataset'''


# TODO: Catch an email exception and flash email failure


@views.route('/admin/approve-shape/<dataset_name>')
@login_required
def approve_shape_view(dataset_name):
    return approve_shape(dataset_name)


def approve_shape(dataset_name):
    meta = session.query(ShapeMetadata).get(dataset_name)
    meta.approved_status = True
    session.commit()

    # Send ingest task to SQS.
    job = {"endpoint": "add_shape", "query": meta.dataset_name}
    ticket = submit_job(job)

    # Create and record task status.
    add_task(meta.dataset_name, ETLType['shapeset'])

    # Let the fans know.
    send_approval_email(meta.human_name, meta.contributor_name,
                        meta.contributor_email)

    return ticket


@views.route('/admin/approve-dataset/<source_url_hash>', methods=['GET', 'POST'])
@login_required
def approve_dataset_view(source_url_hash):
    approve_dataset(source_url_hash)
    return redirect(url_for('views.view_datasets'))


def approve_dataset(source_url_hash):
    # Approve it
    meta = session.query(MetaTable).get(source_url_hash)
    meta.approved_status = True
    session.commit()

    send_approval_email(meta.human_name,
                        meta.contributor_name,
                        meta.contributor_email)

    add_task(meta.dataset_name, ETLType['dataset'])

    job = {"endpoint": "add_dataset", "query": meta.source_url_hash}
    return submit_job(job)


def send_approval_email(dataset_name, contributor_name, contributor_email):
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
http://plenar.io""" % (contributor_name, dataset_name)

    send_mail(subject="Your dataset has been added to Plenar.io",
              recipient=contributor_email, body=msg_body)


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
    if context['is_shapefile']:
        return render_template('submit-shape.html', **context)
    else:
        return render_template('submit-table.html', **context)


def add(context):
    # If is_shapefile query arg is present and not 'false', display shape form.
    shape_arg = request.args.get('is_shapefile', 'false')
    context['is_shapefile'] = shape_arg != 'false'

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
        if is_shapefile:
            if shape_already_submitted(form['dataset_name']):
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

            meta.is_approved = True

            if is_shapefile:
                job = {"endpoint": "add_shape", "query": meta.dataset_name}
                submit_job(job)
                add_task(meta.dataset_name, ETLType['shapeset'])
            else:
                job = {"endpoint": "add_dataset", "query": meta.source_url_hash}
                submit_job(job)
                add_task(meta.dataset_name, ETLType['dataset'])
        else:
            return send_submission_email(meta.human_name,
                                         meta.contributor_name,
                                         meta.contributor_email)
        return view_datasets()


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
    shape = ShapeMetadata.get_by_human_name(name)
    return shape is not None


@views.route('/contribute-thankyou')
def contrib_thankyou():
    context = {}
    return render_template('contribute_thankyou.html', **context)


''' Helpers that can create a new MetaTable instance
    with a filled out submission form and dataset_info
    taken from the source URL.'''


def point_meta_from_submit_form(form, is_approved):
    columns, labels = form_columns(form)
    name = slugify(form['dataset_name'], delim=u'_')[:50]

    metatable = MetaTable(
        url=form['file_url'],
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
        column_names=columns
    )

    session.add(metatable)
    session.commit()
    return metatable


def shape_meta_from_submit_form(form, is_approved):
    md = ShapeMetadata.add(
        human_name=form['dataset_name'],
        source_url=form['file_url'],
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


'''Suggestion helpers.'''

ColumnMeta = namedtuple('ColumnMeta', 'name type_ description')
DescriptionMeta = namedtuple("DescriptionMeta",
                             'human_name attribution description')
ContributorMeta = namedtuple('ContributorMeta', 'name organization email')


def _assert_reachable(url):
    try:
        resp = requests.head(url)
        assert resp.status_code != 404
    except:
        raise RuntimeError('Could not reach URL ' + url)


def is_certainly_html(url):
    head = requests.head(url)
    if head.status_code == 302:
        # Edge case with Dropbox redirects.
        return False
    try:
        return 'text/html' in head.headers['content-type']
    except KeyError:
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
        context.update(DescriptionMeta(None, None, '')._asdict())

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

        return [ColumnMeta(name, type_.__visit_name__.lower(), '')
                for name, type_, _ in column_info]


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
        return [ColumnMeta(c['name'],
                           c['dataTypeName'],
                           c.get('description', None))
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

        blob_url = '{}/download/{}/application/zip' \
            .format(self.url_prefix(), self.four_by_four)
        map_url = '{}/api/geospatial/{}?method=export&format=Shapefile'. \
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
        description = self.metadata.get('description')
        human_name = self.metadata.get('name')
        attribution = self.metadata.get('attribution')

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
    datasets_pending = fetch_pending_tables(MetaTable)
    shapes_pending = fetch_pending_tables(ShapeMetadata)
    datasets = fetch_table_etl_status(ETLType['dataset'])
    shapesets = fetch_table_etl_status(ETLType['shapeset'])

    return render_template('admin/view-datasets.html',
                           datasets_pending=datasets_pending,
                           shapes_pending=shapes_pending,
                           datasets=datasets,
                           shape_datasets=shapesets)


@views.route('/admin/dataset-status/')
@login_required
def dataset_status():
    source_url_hash = request.args.get("source_url_hash")

    name = None
    if source_url_hash:
        name = session.query(MetaTable).get(source_url_hash).dataset_name

    results = fetch_table_etl_status(ETLType['dataset'], name)
    results += fetch_table_etl_status(ETLType['shapeset'], name)

    r = []
    for result in results:
        tb = None
        if result.error:
            tb = result.error \
                .replace('\r\n', '<br />') \
                .replace('\n\r', '<br />') \
                .replace('\n', '<br />') \
                .replace('\r', '<br />')
        d = {
            'human_name': result.human_name,
            'status': result.status,
            'task_id': result.task_id,
            'traceback': tb,
            'date_done': None,
        }

        try:
            d['source_url_hash'] = result.source_url_hash  # Point dataset.
        except AttributeError:
            d['source_url_hash'] = result.dataset_name  # Shape dataset.

        if result.date_done:
            d['date_done'] = result.date_done.strftime('%B %d, %Y %H:%M'),
        r.append(d)
    return render_template('admin/dataset-status.html', results=r, name=name)


class EditShapeForm(Form):
    human_name = StringField('human_name', validators=[DataRequired()])
    description = StringField('description', validators=[DataRequired()])
    attribution = StringField('attribution', validators=[DataRequired()])
    update_freq = SelectField('update_freq',
                              choices=[('daily', 'Daily'),
                                       ('weekly', 'Weekly'),
                                       ('monthly', 'Monthly'),
                                       ('yearly', 'Yearly')],
                              validators=[DataRequired()])

    def validate(self):
        return Form.validate(self)


@views.route('/admin/edit-shape/<dataset_name>', methods=['GET', 'POST'])
@login_required
def edit_shape(dataset_name):
    form = EditShapeForm()
    meta = session.query(ShapeMetadata).get(dataset_name)

    if form.validate_on_submit():
        upd = {
            'human_name': form.human_name.data,
            'description': form.description.data,
            'attribution': form.attribution.data,
            'update_freq': form.update_freq.data,
        }
        session.query(ShapeMetadata) \
            .filter(ShapeMetadata.dataset_name == meta.dataset_name) \
            .update(upd)
        session.commit()

        if not meta.approved_status:
            approve_shape(dataset_name)

        flash('%s updated successfully!' % meta.human_name, 'success')
        return redirect(url_for('views.view_datasets'))
    else:
        pass

    num_rows = meta.num_shapes if meta.num_shapes else 0

    context = {
        'form': form,
        'meta': meta,
        'num_rows': num_rows
    }

    return render_template('admin/edit-shape.html', **context)


@views.route('/update-shape/<dataset_name>')
def update_shape_view(dataset_name):
    return queue_update_shape(dataset_name)


def queue_update_shape(dataset_name):
    job = {"endpoint": "update_shape", "query": dataset_name}
    ticket = submit_job(job)
    return make_response(json.dumps({'status': 'success', 'ticket': ticket}))


class EditDatasetForm(Form):
    """ 
    Form to edit meta_master information for a dataset
    """
    human_name = StringField('human_name', validators=[DataRequired()])
    description = StringField('description', validators=[DataRequired()])
    attribution = StringField('attribution', validators=[DataRequired()])
    update_freq = SelectField('update_freq',
                              choices=[('daily', 'Daily'),
                                       ('weekly', 'Weekly'),
                                       ('monthly', 'Monthly'),
                                       ('yearly', 'Yearly')],
                              validators=[DataRequired()])
    observed_date = StringField('observed_date', validators=[DataRequired()])
    latitude = StringField('latitude')
    longitude = StringField('longitude')
    location = StringField('location')

    def validate(self):
        rv = Form.validate(self)
        if not rv:
            return False

        valid = True

        if not self.location.data and (not self.latitude.data or not self.longitude.data):
            valid = False
            self.location.errors.append(
                'You must either provide a Latitude and Longitude field name or a Location field name')

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
        session.query(MetaTable) \
            .filter(MetaTable.source_url_hash == meta.source_url_hash) \
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
def delete_dataset_view(source_url_hash):
    return delete_dataset(source_url_hash)


def delete_dataset(source_url_hash):
    ticket = submit_job({'endpoint': 'delete_dataset', 'query': source_url_hash})
    return make_response(json.dumps({'status': 'success', 'ticket': ticket}))


@views.route('/update-dataset/<source_url_hash>')
def update_dataset_view(source_url_hash):
    return queue_update_dataset(source_url_hash)


def queue_update_dataset(source_url_hash):
    job = {"endpoint": "update_dataset", "query": source_url_hash}
    ticket = submit_job(job)
    return make_response(json.dumps({'status': 'success', 'ticket': ticket}))


@views.route('/check-update/<ticket>')
def check_update(ticket):
    r = {'status': get_status(ticket)}
    resp = make_response(json.dumps(r))
    resp.headers['Content-Type'] = 'application/json'
    return resp


''' Shape monitoring '''


@views.route('/admin/shape-status/')
@login_required
def shape_status():
    table_name = request.args['dataset_name']
    shape_meta = fetch_table_etl_status(ETLType['shapeset'], table_name)[0]
    return render_template('admin/shape-status.html', shape=shape_meta)


@views.route('/admin/delete-shape/<table_name>')
@login_required
def delete_shape_view(table_name):
    return delete_shape(table_name)


def delete_shape(table_name):
    ticket = submit_job({'endpoint': 'delete_shape', 'query': table_name})
    return make_response(json.dumps({'status': 'success', 'ticket': ticket}))
