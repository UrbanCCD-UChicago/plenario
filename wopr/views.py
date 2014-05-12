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
    return render_template('grid.html')

@views.route('/map/')
def map_view():
    return render_template('map.html')
