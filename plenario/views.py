from flask import make_response, request, render_template, current_app, g, \
    Blueprint
from plenario.models import MasterTable, MetaTable
from plenario.database import session
from plenario.utils.helpers import get_socrata_data_info
from flask_login import login_required
from datetime import datetime, timedelta
from urlparse import urlparse
import requests
from flask_login import login_required

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
