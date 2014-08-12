from flask import make_response, request, render_template, current_app, g, \
    Blueprint
from plenario.models import MasterTable, MetaTable
from plenario.database import session
from plenario.utils.helpers import get_socrata_data_info
from flask_login import login_required
from datetime import datetime, timedelta
from urlparse import urlparse
import requests

views = Blueprint('views', __name__)

@views.route('/')
def index():
    return render_template('index.html')

@views.route('/grid')
def grid_view():
    context = {}
    context['datasets'] = session.query(MetaTable).all()
    for dataset in context['datasets']:
        if not dataset.obs_to or not dataset.obs_from:
            # Arbitrarily setting obs_to and obs_from if they are not present.
            dataset.obs_from = datetime.now() - timedelta(days=365 * 4)
            dataset.obs_to = datetime.now()
    context['default'] = [d for d in context['datasets'] if d.dataset_name == 'chicago_crimes_all'][0]
    context['resolutions'] = {
        300: '~300m', 
        400: '~400m', 
        500: '~500m', 
        750: '~750m', 
        1000: '~1km', 
        2000: '~2km', 
        3000: '~3km', 
        4000: '~4km', 
        5000: '~5km'
    }
    context['operators'] = {
        'is': 'eq',
    }
    return render_template('grid.html', **context)

@views.route('/explore/')
def explore_view():
    return render_template('explore.html')

@views.route('/explore/detail')
def explore_detail_view():
    return render_template('explore_detail.html')

@views.route('/api-docs')
def api_docs_view():
    return render_template('api-docs.html')

@views.route('/about')
def about_view():
    return render_template('about.html')

@login_required
@views.route('/add-dataset', methods=['GET', 'POST'])
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

@login_required
@views.route('/view-datasets')
def view_datasets():
    datasets = session.query(MetaTable).all()
    return render_template('view-datasets.html', datasets=datasets)
