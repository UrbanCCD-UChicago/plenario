import json

from flask import request
from sqlalchemy import func

from plenario.api.blueprints import api
from plenario.api.common import cache, CACHE_TIMEOUT, crossdomain, make_cache_key
from plenario.fields import Commalist, DateTime, Geometry, Pointset
from plenario.filters import intersects
from plenario.models import MetaTable
from plenario.response import parser, ok
from plenario.server import db

parameters = {
    'dataset_name': Pointset(missing=None),
    'dataset_name__in': Commalist(Pointset, missing=None),
    'location_geom__within': Geometry(missing=None),
    'obs_date__ge': DateTime(missing=None),
    'obs_date__le': DateTime(missing=None)
}


# TODO(heyzoos) NOQA
# TODO(heyzoos) DateTime result format
# TODO(heyzoos) When given the dataset query arg, it is not serialized properly
@api.route('/v1/api/datasets')
# @cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin='*')
@parser.use_args(parameters)
def metatables(args):
    """Return metadata about datasets we have ingested."""

    dataset = args['dataset_name']
    datasets = args['dataset_name__in']
    geom = args['location_geom__within']
    startdt = args['obs_date__ge']
    enddt = args['obs_date__le']

    columns = list(MetaTable.__table__.c) + [func.ST_AsGeoJson(MetaTable.bbox)]

    query = db.session.query(*columns)
    query = query.filter(MetaTable.date_added.isnot(None))
    query = query.filter(MetaTable.dataset_name == dataset.name) if dataset is not None else query
    query = query.filter(MetaTable.dataset_name.in_((t.name for t in datasets))) if datasets else query
    query = query.filter(MetaTable.obs_from >= startdt) if startdt else query
    query = query.filter(MetaTable.obs_to <= enddt) if enddt else query
    query = query.filter(intersects(MetaTable, geom)) if geom else query

    payload = ok(request.args, [transform(row) for row in query])
    db.session.commit()

    return payload


def transform(row):
    """Transforms query results for the metatables view."""

    result = row._asdict()
    result['columns'] = [{'field_name': k, 'field_type': v} for k, v in row.column_names.items()]
    result['bbox'] = json.loads(row[-1]) if row[-1] else None
    del result['column_names']
    return result
