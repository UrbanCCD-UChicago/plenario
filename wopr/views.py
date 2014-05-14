from flask import make_response, request, render_template, current_app, g, \
    Blueprint
from wopr.models import MasterTable, MetaTable
from wopr.database import session

views = Blueprint('views', __name__)

@views.route('/')
def index():
    return render_template('index.html')

@views.route('/grid/')
def grid_view():
    context = {}
    context['datasets'] = session.query(MetaTable).all()
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

@views.route('/map/')
def map_view():
    return render_template('map.html')
