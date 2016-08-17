"""endpoints: A set of dictionaries that map tickets to the
actual work that needs to be done."""


from plenario.api.point import _timeseries, _detail, _detail_aggregate, _meta
from plenario.api.point import _grid, _datadump
from plenario.api.shape import _aggregate_point_data, _export_shape
from plenario.tasks import add_dataset, delete_dataset, update_dataset
from plenario.tasks import add_shape, update_shape, delete_shape
from plenario.tasks import update_weather, frequency_update

endpoint_logic = {
    # /timeseries?<args>
    'timeseries': lambda args: _timeseries(args),
    # /detail-aggregate?<args>
    'detail-aggregate': lambda args: _detail_aggregate(args),
    # /detail?<args>
    # emulating row-removal features of _detail_response. Not very DRY, but it's the cleanest option.
    'detail': lambda args: [{key: row[key] for key in row.keys()
                             if key not in ['point_date', 'hash', 'geom']} for row in
                            _detail(args)],
    # /datasets?<args>
    'meta': lambda args: _meta(args),
    # /fields/<dataset>
    'fields': lambda args: _meta(args),
    # /grid?<args>
    'grid': lambda args: _grid(args),
    'datadump': lambda args: _datadump(args),
    # Health endpoint.
    # 'ping': lambda args: {'hello': 'from worker {}'.format(worker_id)}
    'ping': lambda args: {'hello': 'from a worker!'}
}

shape_logic = {
    # /shapes/<shape>?<args>
    'export-shape': lambda args: _export_shape(args),
    # /shapes/<dataset>/<shape>?<args>
    'aggregate-point-data': lambda args: [{key: row[key] for key in row.keys()
                                           if key not in ['hash', 'ogc_fid']}
                                          for row in _aggregate_point_data(args)]
}

etl_logic = {
    'add_dataset': lambda args: add_dataset(args),
    'update_dataset': lambda args: update_dataset(args),
    'delete_dataset': lambda args: delete_dataset(args),
    'add_shape': lambda args: add_shape(args),
    'update_shape': lambda args: update_shape(args),
    'delete_shape': lambda args: delete_shape(args),
    "update_weather": lambda: update_weather(),
    "frequency_update": lambda args: frequency_update(args)
}
