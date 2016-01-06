from plenario.api.common import cache, CACHE_TIMEOUT, make_cache_key, crossdomain, dthandler, RESPONSE_LIMIT
from plenario.utils.helpers import get_size_in_degrees
from plenario.database import session, app_engine as engine, Base
from flask import request, make_response
from sqlalchemy import Table, func
import json
import shapely.wkb, shapely.geometry

@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def weather_stations():
    #print "weather_stations()"
    raw_query_params = request.args.copy()
    #print "weather_stations(): raw_query_params=", raw_query_params

    stations_table = Table('weather_stations', Base.metadata,
        autoload=True, autoload_with=engine, extend_existing=True)
    valid_query, query_clauses, resp, status_code = make_query(stations_table,raw_query_params)
    if valid_query:
        resp['meta']['status'] = 'ok'
        base_query = session.query(stations_table)
        for clause in query_clauses:
            print "weather_stations(): filtering on clause", clause
            base_query = base_query.filter(clause)
        values = [r for r in base_query.all()]
        fieldnames = [f for f in stations_table.columns.keys()]
        for value in values:
            d = {f:getattr(value, f) for f in fieldnames}
            loc = str(value.location)
            d['location'] = shapely.wkb.loads(loc.decode('hex')).__geo_interface__
            resp['objects'].append(d)
    resp['meta']['query'] = raw_query_params
    resp = make_response(json.dumps(resp, default=dthandler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp

@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def weather(table):
    raw_query_params = request.args.copy()

    weather_table = Table('dat_weather_observations_%s' % table, Base.metadata,
        autoload=True, autoload_with=engine, extend_existing=True)
    stations_table = Table('weather_stations', Base.metadata,
        autoload=True, autoload_with=engine, extend_existing=True)
    valid_query, query_clauses, resp, status_code = make_query(weather_table,raw_query_params)
    if valid_query:
        resp['meta']['status'] = 'ok'
        base_query = session.query(weather_table, stations_table)\
            .join(stations_table,
            weather_table.c.wban_code == stations_table.c.wban_code)
        for clause in query_clauses:
            base_query = base_query.filter(clause)

        try:
            base_query = base_query.order_by(getattr(weather_table.c, 'date').desc())
        except AttributeError:
            base_query = base_query.order_by(getattr(weather_table.c, 'datetime').desc())
        base_query = base_query.limit(RESPONSE_LIMIT) # returning the top 1000 records
        if raw_query_params.get('offset'):
            offset = raw_query_params['offset']
            base_query = base_query.offset(int(offset))
        values = [r for r in base_query.all()]
        weather_fields = weather_table.columns.keys()
        station_fields = stations_table.columns.keys()
        weather_data = {}
        station_data = {}
        for value in values:
            wd = {f: getattr(value, f) for f in weather_fields}
            sd = {f: getattr(value, f) for f in station_fields}
            if weather_data.get(value.wban_code):
                weather_data[value.wban_code].append(wd)
            else:
                weather_data[value.wban_code] = [wd]
            loc = str(value.location)
            sd['location'] = shapely.wkb.loads(loc.decode('hex')).__geo_interface__
            station_data[value.wban_code] = sd
        for station_id in weather_data.keys():
            d = {
                'station_info': station_data[station_id],
                'observations': weather_data[station_id],
            }
            resp['objects'].append(d)
        resp['meta']['total'] = sum([len(r['observations']) for r in resp['objects']])
    resp['meta']['query'] = raw_query_params
    resp = make_response(json.dumps(resp, default=dthandler), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp

'''
make_query is a holdover from the old API implementation that used Master Table
'''

def make_query(table, raw_query_params):
    table_keys = table.columns.keys()
    args_keys = raw_query_params.keys()
    resp = {
        'meta': {
            'status': 'error',
            'message': '',
        },
        'objects': [],
    }
    status_code = 200
    query_clauses = []
    valid_query = True

    #print "make_query(): args_keys = ", args_keys

    if 'offset' in args_keys:
        args_keys.remove('offset')
    if 'limit' in args_keys:
        args_keys.remove('limit')
    if 'order_by' in args_keys:
        args_keys.remove('order_by')
    if 'weather' in args_keys:
        args_keys.remove('weather')
    for query_param in args_keys:
        try:
            field, operator = query_param.split('__')
            #print "make_query(): field, operator =", field, operator
        except ValueError:
            field = query_param
            operator = 'eq'
        query_value = raw_query_params.get(query_param)
        column = table.columns.get(field)
        if field not in table_keys:
            resp['meta']['message'] = '"%s" is not a valid fieldname' % field
            status_code = 400
            valid_query = False
        elif operator == 'in':
            query = column.in_(query_value.split(','))
            query_clauses.append(query)
        elif operator == 'within':
            geo = json.loads(query_value)
            #print "make_query(): geo is", geo.items()
            if 'features' in geo.keys():
                val = geo['features'][0]['geometry']
            elif 'geometry' in geo.keys():
                val = geo['geometry']
            else:
                val = geo
            if val['type'] == 'LineString':
                shape = shapely.geometry.asShape(val)
                lat = shape.centroid.y
                # 100 meters by default
                x, y = get_size_in_degrees(100, lat)
                val = shape.buffer(y).__geo_interface__
            val['crs'] = {"type":"name","properties":{"name":"EPSG:4326"}}
            query = column.ST_Within(func.ST_GeomFromGeoJSON(json.dumps(val)))
            #print "make_query: val=", val
            #print "make_query(): query = ", query
            query_clauses.append(query)
        elif operator.startswith('time_of_day'):
            if operator.endswith('ge'):
                query = func.date_part('hour', column).__ge__(query_value)
            elif operator.endswith('le'):
                query = func.date_part('hour', column).__le__(query_value)
            query_clauses.append(query)
        else:
            try:
                attr = filter(
                    lambda e: hasattr(column, e % operator),
                    ['%s', '%s_', '__%s__']
                )[0] % operator
            except IndexError:
                resp['meta']['message'] = '"%s" is not a valid query operator' % operator
                status_code = 400
                valid_query = False
                break
            if query_value == 'null': # pragma: no cover
                query_value = None
            query = getattr(column, attr)(query_value)
            query_clauses.append(query)

    #print "make_query(): query_clauses=", query_clauses
    return valid_query, query_clauses, resp, status_code
