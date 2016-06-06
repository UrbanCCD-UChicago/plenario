import json
import shapely.wkb

from datetime import datetime
from flask import make_response

from plenario.api.common import make_csv, dthandler, unknownObjectHandler
from plenario.api.errors import internal_error
from plenario.api.filters import FilterMaker
from plenario.database import session


def json_response_base(validator, objects, query=''):
    meta = {
        'status': 'ok',
        'message': '',
        'query': query,
    }

    if validator:
        meta['message'] = validator.warnings
        meta['query'] = validator.vals

    return {
        'meta': meta,
        'objects': objects,
    }


def geojson_response_base():
    return {
        "type": "FeatureCollection",
        "features": []
    }


def add_geojson_feature(geojson_response, feature_geom, feature_properties):
    new_feature = {
        "type": "Feature",
        "geometry": feature_geom,
        "properties": feature_properties
    }
    geojson_response['features'].append(new_feature)


def form_json_detail_response(to_remove, validator, rows):
    to_remove.append('geom')
    remove_columns_from_dict(rows, to_remove)
    resp = json_response_base(validator, rows)
    resp['meta']['total'] = len(resp['objects'])
    resp['meta']['query'] = validator.vals
    resp = make_response(json.dumps(resp, default=unknownObjectHandler), 200)
    resp.headers['Content-Type'] = 'application/json'
    return resp


def form_csv_detail_response(to_remove, validator, rows):
    to_remove.append('geom')
    remove_columns_from_dict(rows, to_remove)

    # Column headers from arbitrary row,
    # then the values from all the others
    csv_resp = [rows[0].keys()] + [row.values() for row in rows]
    resp = make_response(make_csv(csv_resp), 200)
    dname = validator.dataset.name  # dataset_name
    filedate = datetime.now().strftime('%Y-%m-%d')
    resp.headers['Content-Type'] = 'text/csv'
    resp.headers['Content-Disposition'] = 'attachment; filename=%s_%s.csv' % (dname, filedate)
    return resp


def form_geojson_detail_response(to_remove, validator, rows):
    geojson_resp = geojson_response_base()
    # We want the geom this time.
    remove_columns_from_dict(rows, to_remove)

    for row in rows:
        try:
            wkb = row.pop('geom')
            geom = shapely.wkb.loads(wkb.desc, hex=True).__geo_interface__
        except (KeyError, AttributeError):
            # If we couldn't fund a geom value,
            # or said value was not of the expected type,
            # then skip this column
            continue
        else:
            add_geojson_feature(geojson_resp, geom, row)

    resp = make_response(json.dumps(geojson_resp, default=dthandler), 200)
    resp.headers['Content-Type'] = 'application/json'
    return resp


def form_detail_sql_query(validator, aggregate_points=False):
    dset = validator.dataset
    try:
        q = session.query(dset)
        if validator.conditions:
            q = q.filter(*validator.conditions)
    except Exception as e:
        return internal_error('Failed to construct column filters.', e)

    try:
        # Add time filters
        maker = FilterMaker(validator.vals, dataset=dset)
        q = q.filter(*maker.time_filters())

        # Add geom filter, if provided
        geom = validator.get_geom()
        if geom is not None:
            geom_filter = maker.geom_filter(geom)
            q = q.filter(geom_filter)
    except Exception as e:
        return internal_error('Failed to construct time and geometry filters.', e)

    # if the query specified a shape dataset, add a join to the sql query with that dataset
    shape_table = validator.vals.get('shape')
    if shape_table is not None:
        shape_columns = ['{}.{} as {}'.format(shape_table.name, col.name, col.name) for col in shape_table.c]
        if aggregate_points:
            q = q.from_self(shape_table).filter(dset.c.geom.ST_Intersects(shape_table.c.geom)).group_by(shape_table)
        else:
            q = q.join(shape_table, dset.c.geom.ST_Within(shape_table.c.geom))
            # add columns from shape dataset to the select statement
            q = q.add_columns(*shape_columns)

    return q


def remove_columns_from_dict(rows, col_names):
    for row in rows:
        for name in col_names:
            del row[name]
