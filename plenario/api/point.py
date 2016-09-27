import os
import json
import re
import math
import random
import shapely.geometry
import shapely.wkb
import copy
import sqlalchemy
import traceback
import warnings

import response as api_response

from collections import OrderedDict
from datetime import datetime
from flask import request, Response
from dateutil import parser
from plenario.api.common import cache, crossdomain, CACHE_TIMEOUT
from plenario.api.common import make_cache_key, unknown_object_json_handler
from plenario.api.condition_builder import parse_tree
from plenario.api.validator import DatasetRequiredValidator, NoGeoJSONDatasetRequiredValidator
from plenario.api.validator import NoDefaultDatesValidator, validate, NoGeoJSONValidator, has_tree_filters, converters
from plenario.api.jobs import make_job_response, submit_job, get_job, set_status, get_status, set_request, get_request, \
    get_result, set_flag, get_flag
from plenario.models import MetaTable, DataDump
from plenario.database import fast_count, windowed_query

# Use the standard pool if this is just the app,
# but use the shared connection pool if this
# is the worker. It's more efficient!
if os.environ.get('WORKER'):
    from worker import session
else:
    from plenario.database import session


# ======
# routes
# ======

# The get_job method in jobs.py does not have crossdomain, and is out of the
# Flask context. So we define a wrapper here to access it.
@crossdomain(origin="*")
def get_job_view(ticket):
    return get_job(ticket)


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def timeseries():
    fields = ('location_geom__within', 'dataset_name', 'dataset_name__in',
              'agg', 'obs_date__ge', 'obs_date__le', 'data_type', 'job')
    validator = NoGeoJSONValidator(only=fields)
    validator_result = validate(validator, request.args.to_dict())

    if validator_result.errors:
        return api_response.bad_request(validator_result.errors)

    if validator_result.data.get('job'):
        return make_job_response("timeseries", validator_result)
    else:
        panel = _timeseries(validator_result)
        return api_response.timeseries_response(panel, validator_result)


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def detail_aggregate():
    fields = ('location_geom__within', 'dataset_name', 'agg', 'obs_date__ge',
              'obs_date__le', 'data_type', 'job')
    validator = NoGeoJSONDatasetRequiredValidator(only=fields)
    validator_result = validate(validator, request.args.to_dict())

    if validator_result.errors:
        return api_response.bad_request(validator_result.errors)

    if validator_result.data.get('job'):
        return make_job_response("detail-aggregate", validator_result)
    else:
        time_counts = _detail_aggregate(validator_result)
        return api_response.detail_aggregate_response(time_counts, validator_result)


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def detail():
    fields = ('location_geom__within', 'dataset_name', 'shape', 'obs_date__ge',
              'obs_date__le', 'data_type', 'offset', 'date__time_of_day_ge',
              'date__time_of_day_le', 'limit', 'job')
    validator = DatasetRequiredValidator(only=fields)
    validator_result = validate(validator, request.args.to_dict())

    if validator_result.errors:
        return api_response.bad_request(validator_result.errors)

    if validator_result.data.get('job'):
        return make_job_response("detail", validator_result)
    else:
        result_rows = _detail(validator_result)
        return api_response.detail_response(result_rows, validator_result)


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def datadump():
    fields = ('location_geom__within', 'dataset_name', 'shape', 'obs_date__ge',
              'obs_date__le', 'offset', 'date__time_of_day_ge',
              'date__time_of_day_le', 'limit', 'job')
    validator = DatasetRequiredValidator(only=fields)
    validator_result = validate(validator, request.args.to_dict())

    # Get origin IP for Denial-of-Service protection
    origin_ip = request.headers.get("X-Forwarded-For")
    if not origin_ip:
        origin_ip = request.remote_addr

    # Keep URL root to form download link later
    validator_result.data["datadump_urlroot"] = request.url_root

    # Set job to True just to be canonical
    validator_result.data["job"] = True

    job = make_job_response("datadump", validator_result)
    log("===== DATADUMP {} REQUESTED BY {} =====".format(json.loads(job.get_data())["ticket"], origin_ip))
    return job


@crossdomain(origin="*")
def get_datadump(ticket):
    job = get_job(ticket)
    try:
        if not "error" in json.loads(job.get_data()) and get_status(ticket)["status"] == "success":
            datatype = request.args.get("data_type") if request.args.get("data_type") and request.args.get(
                "data_type") in ["json", "csv"] else "json"

            # Send data from Postgres in sequence in JSON format
            def stream_json():
                counter = 0
                row = session.query(DataDump).filter(
                    sqlalchemy.and_(DataDump.request == ticket, DataDump.part == counter)).one()
                # Make headers for JSON.
                metadata = json.loads(row.get_data())
                yield """{{"startTime": "{}", "endTime": "{}", "workers": {}, "data": [""".format(
                    metadata["startTime"], metadata["endTime"], json.dumps(metadata["workers"]))
                counter += 1
                # The "total" in the status lists the largest part number.
                # In reality, there are total+1 parts because the header occupies part 0.
                # So we also want to go up to the total as well. Don't worry,
                # logic in the _datadump ensures that total is the largest part number.
                while counter <= get_status(ticket)["progress"]["total"]:
                    # Using one() here in order to assert quality data
                    # If one() fails (not that it should) then that means
                    # that the datadump is bad and should not be served.
                    row = session.query(DataDump).filter(
                        sqlalchemy.and_(DataDump.request == ticket, DataDump.part == counter)).one()
                    # Return result with the list brackets [] sliced off.
                    yield row.get_data()[1:-1]
                    if counter < get_status(ticket)["progress"]["total"]: yield ","
                    counter += 1
                # Finish off JSON syntax
                yield "]}"

            # Send data from Postgres in sequence in CSV format
            def stream_csv():
                counter = 0
                row = session.query(DataDump).filter(
                    sqlalchemy.and_(DataDump.request == ticket, DataDump.part == counter)).one()
                # Make headers for CSV.
                metadata = json.loads(row.get_data())
                columns = [str(c) for c in metadata["columns"]]
                # Uncomment to enable CSV metadata
                # yield "# STARTTIME: {}\n# ENDTIME: {}\n# WORKERS: {}\n".format(metadata["startTime"], metadata["endTime"], ", ".join(metadata["workers"]))
                yield ",".join([json.dumps(column) for column in columns]) + "\n"
                counter += 1
                while counter <= get_status(ticket)["progress"]["total"]:
                    row = session.query(DataDump).filter(
                        sqlalchemy.and_(DataDump.request == ticket, DataDump.part == counter)).one()
                    for csvrow in json.loads(row.get_data()):
                        yield ",".join(
                            [json.dumps(csvrow[key].encode("utf-8")) if type(
                                csvrow[key]) is unicode else json.dumps(str(csvrow[key]))
                             for key in columns]) + "\n"
                    counter += 1

            # Set the streaming generator (JSON by default)
            stream_data = stream_json
            if datatype == "csv":
                stream_data = stream_csv

            response = Response(stream_data(), mimetype="text/{}".format(datatype))
            response.headers["Content-Disposition"] = "attachment; filename=\"{}.datadump.{}\"".format(
                get_request(ticket)["query"]["dataset"], datatype)
            return response
        else:
            return job
    except:
        traceback.print_exc()
        return job


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def grid():
    fields = ('dataset_name', 'resolution', 'buffer', 'obs_date__le', 'obs_date__ge',
              'location_geom__within', 'job')
    validator_result = validate(DatasetRequiredValidator(only=fields), request.args.to_dict())

    if validator_result.errors:
        return api_response.bad_request(validator_result.errors)

    if validator_result.data.get('job'):
        return make_job_response("grid", validator_result)
    else:
        result_data = _grid(validator_result)
        return api_response.grid_response(result_data)


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def dataset_fields(dataset_name):
    request_args = request.args.to_dict()
    request_args['dataset_name'] = dataset_name
    fields = ('obs_date__le', 'obs_date__ge', 'dataset_name', 'job')
    validator = DatasetRequiredValidator(only=fields)
    validator_result = validate(validator, request_args)

    if validator_result.errors:
        return api_response.bad_request(validator_result.errors)

    if validator_result.data.get('job'):
        return make_job_response("fields", validator_result)
    else:
        result_data = _meta(validator_result)
        return api_response.fields_response(result_data, validator_result)


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def meta():
    fields = ('obs_date__le', 'obs_date__ge', 'dataset_name', 'location_geom__within', 'job')
    validator_result = validate(NoDefaultDatesValidator(only=fields), request.args.to_dict())

    if validator_result.errors:
        return api_response.bad_request(validator_result.errors)

    if validator_result.data.get('job'):
        return make_job_response("meta", validator_result)
    else:
        result_data = _meta(validator_result)
        return api_response.meta_response(result_data, validator_result)


# ============
# _route logic
# ============


def _timeseries(args):
    meta_params = ['geom', 'dataset', 'dataset_name__in', 'obs_date__ge', 'obs_date__le', 'agg']
    meta_vals = [args.data.get(k) for k in meta_params]
    geom, dataset, table_names, start_date, end_date, agg = meta_vals

    ctrees = {}

    if has_tree_filters(args.data):
        # Timeseries is a little tricky. If there aren't filters,
        # it would be ridiculous to build a condition tree for every one.
        for field, value in args.data.items():
            if 'filter' in field:
                # This pattern matches the last occurrence of the '__' pattern.
                # Prevents an error that is caused by dataset names with trailing
                # underscores.
                tablename = re.split(r'__(?!_)', field)[0]
                metarecord = MetaTable.get_by_dataset_name(tablename)
                pt = metarecord.point_table
                ctrees[pt.name] = parse_tree(pt, value)
        # Just cleanliness, since we don't use this argument. Doesn't have
        # to show up in the JSON response.
        del args.data['dataset']

    # If no dataset_name__in list was provided, have to fill it in by invoking
    # MetaTable.index() here! Not in the validator. This way the list stays up
    # to date.
    if table_names is None:
        table_names = MetaTable.index()
        args.data['dataset_name__in'] = table_names

    # If a single dataset was provided, it's the only thing we need to consider.
    if dataset is not None:
        table_names = [dataset.name]
        del args.data['dataset_name__in']

    # remove table names which wouldn't return anything for the query, given
    # the time and geom constraints
    try:
        table_names = MetaTable.narrow_candidates(table_names, start_date, end_date, geom)
    except Exception as e:
        traceback.print_exc()
        msg = 'Failed to gather candidate tables.'
        return api_response.make_raw_error("{}: {}".format(msg, e))
        # TODO: Correctly handle _timeseries (and all the other endpoints)
        # TODO: so that make_error is called when there is an error.

    # If there aren't any table names, it causes an error down the code. Better
    # to return and inform them that the request wouldn't have found anything.
    if not table_names:
        return api_response.bad_request("Your request doesn't return any results. Try "
                                        "adjusting your time constraint or location "
                                        "parameters.")

    try:
        panel = MetaTable.timeseries_all(
            table_names, agg, start_date, end_date, geom, ctrees
        )
    except Exception as e:
        msg = 'Failed to construct timeseries.'
        return api_response.make_raw_error("{}: {}".format(msg, e))

    return MetaTable.attach_metadata(panel)


def _detail_aggregate(args):
    """Returns a record for every row in the specified dataset with brief
    temporal and spatial information about the row. This can give a user of the
    platform a quick overview about what is available within their constraints.

    :param args: dictionary of request arguments

    :returns: csv or json response object"""

    meta_params = ('obs_date__ge', 'obs_date__le', 'agg', 'geom', 'dataset')
    meta_vals = (args.data.get(k) for k in meta_params)
    start_date, end_date, agg, geom, dataset = meta_vals

    time_counts = []

    if not has_tree_filters(args.data):
        # The obs_date arguments set the bounds of all the aggregates.
        # We don't want to create a condition tree that has point_date filters.
        args.data[dataset.name + '__filter'] = request_args_to_condition_tree(
            args.data, ignore=['obs_date__ge', 'obs_date__le']
        )

    dataset_conditions = {k: v for k, v in args.data.items() if 'filter' in k}
    for tablename, condition_tree in dataset_conditions.items():
        # This pattern matches the last occurrence of the '__' pattern.
        # Prevents an error that is caused by dataset names with trailing
        # underscores.
        tablename = re.split(r'__(?!_)', tablename)[0]
        table = MetaTable.get_by_dataset_name(tablename).point_table
        try:
            conditions = parse_tree(table, condition_tree)
        except ValueError:  # Catches empty condition tree.
            conditions = None

        try:
            ts = MetaTable.get_by_dataset_name(table.name).timeseries_one(
                agg, start_date, end_date, geom, conditions
            )
        except Exception as e:
            msg = 'Failed to construct timeseries'
            return api_response.make_raw_error("{}: {}".format(msg, e))

        time_counts += [{'count': c, 'datetime': d} for c, d in ts[1:]]

    return time_counts


def _detail(args):
    meta_params = ('dataset', 'shape', 'data_type', 'limit', 'offset')
    meta_vals = (args.data.get(k) for k in meta_params)
    dataset, shapeset, data_type, limit, offset = meta_vals

    q = detail_query(args).order_by(dataset.c.point_date.desc())

    # Apply limit and offset.
    q = q.limit(limit)
    q = q.offset(offset) if offset else q

    try:
        columns = [c.name for c in dataset.columns]
        if shapeset:
            columns += [c.name for c in shapeset.columns]
        return [OrderedDict(zip(columns, row)) for row in q.all()]
    except Exception as e:
        session.rollback()
        msg = "Failed to fetch records."
        return api_response.make_raw_error("{}: {}".format(msg, e))


def _datadump(args):
    """Chunk and store an arbitrarily large query result to be delivered to
    users through a download. Defaults to returning observations from Plenario
    databases.

    :param args: (ValidatorResult) carries all meta information
    :returns: (ValidatorResult) with download link defined"""

    requestid = args.data.get("jobsframework_ticket")

    chunksize = 1000
    original_validated = copy.deepcopy(args)

    # Number of rows to exceed in order to start
    # deferring job (to allow other jobs to complete first)
    row_threshold = 100000

    query = original_validated.data

    # Calculate query parameters
    log("===== STARTING WORK ON DATADUMP {} =====".format(args.data["jobsframework_ticket"]))
    log("-> Query: {}".format(json.dumps(query, default=unknown_object_json_handler)))
    rows = fast_count(detail_query(args))
    log("-> Dump contains {} rows.".format(rows))

    if rows > row_threshold:
        # Bake-in "niceness" in datadump;
        # Since datadumps can be time-consuming,
        # throttle them by deferring 50% of them for
        # 10 seconds later (letting other jobs run).
        if random.random() < 0.5:
            log("-> Deferring dump due to size.")
            return {"jobsframework_metacommands": ["defer", {"setTimeout": 10}]}

    chunks = int(math.ceil(rows / float(chunksize)))

    status = get_status(requestid)
    status["progress"] = {"done": 0, "total": chunks}
    set_status(requestid, status)

    q = detail_query(args)
    # TODO: Generalize the "dataset" argument, right now it expects Plenario DB
    columns = [c.name for c in args.data.get('dataset').columns if c.name not in ['point_date', 'hash', 'geom']]

    def add_chunk(chunk):
        chunk = [OrderedDict(zip(columns, row)) for row in chunk]
        chunk = [{column: row[column] for column in columns} for row in chunk]
        dump = DataDump(os.urandom(16).encode('hex'), requestid, part, chunks,
                        json.dumps(chunk, default=unknown_object_json_handler))
        session.add(dump)
        try:
            session.commit()
        except Exception as e:
            session.rollback()
            print "DATADUMP ERROR: {}".format(e)
            traceback.print_exc()
            raise e

        status = get_status(requestid)
        status["progress"] = {"done": part, "total": chunks}
        set_status(requestid, status)

        # Supress datadump cleanup
        set_flag(requestid + "_suppresscleanup", True, 10800)

    part = 0
    chunk = []
    count = 0
    for row in windowed_query(q, args.data.get('dataset').c.point_date, chunksize):
        chunk.append(row)
        count += 1
        if count >= chunksize:
            count = 0
            part += 1
            add_chunk(chunk)
            chunk = []
    # Finish leftovers
    if len(chunk) > 0:
        part += 1
        add_chunk(chunk)

    metadata = """{{"startTime": "{}", "endTime": "{}", "workers": {}, "columns": {}}}""".format(
        get_status(requestid)["meta"]["startTime"], str(datetime.now()),
        json.dumps([args.data["jobsframework_workerid"]]),
        json.dumps(columns))
    dump = DataDump(requestid, requestid, 0, chunks, metadata)
    session.add(dump)
    try:
        session.commit()
    except Exception as e:
        session.rollback()
        print "DATADUMP ERROR: {}".format(e)
        traceback.print_exc()
        raise e

    log("===== DATADUMP COMPLETE =====")

    return {"url": args.data["datadump_urlroot"] + "v1/api/datadump/" + requestid}


# Datadump utilities =======================
def cleanup_datadump():
    def _cleanup_datadump(requestid):
        try:
            session.query(DataDump).filter(DataDump.request == requestid).delete()
            session.commit()
            log("---> Removed request {} from database.".format(requestid))
        except Exception as e:
            session.rollback()
            traceback.print_exc()
            log("---> Problem while clearing datadump request: {}".format(e))
            print "ERROR IN DATADUMP: COULD NOT CLEAN UP:", e

    for requestid, in session.query(DataDump.request).distinct():
        print(requestid)
        if not get_flag(requestid + "_suppresscleanup"):
            _cleanup_datadump(requestid)


def log(msg):
    try:
        logfile = open("/opt/python/log/api.log", "a")
    except IOError:
        warnings.warn("/opt/python/log/api.log not found - writing to current "
                      "directory", RuntimeWarning)
        logfile = open("./api.log", "a")
    logfile.write("{} - {}\n".format(datetime.now(), msg))
    logfile.close()


# ==========================================


def detail_query(args, aggregate=False):
    meta_params = ('dataset', 'shapeset', 'data_type', 'geom', 'obs_date__ge',
                   'obs_date__le')
    meta_vals = (args.data.get(k) for k in meta_params)
    dataset, shapeset, data_type, geom, obs_date__ge, obs_date__le = meta_vals

    # If there aren't tree filters provided, a little formatting is needed
    # to make the general filters into an 'and' tree.
    if not has_tree_filters(args.data):
        # Creates an AND condition tree and adds it to args.
        args.data[dataset.name + '__filter'] = request_args_to_condition_tree(
            request_args=args.data,
            ignore=['shapeset']
        )

    # Sort out the filter conditions from the rest of the user arguments.
    filters = {k: v for k, v in args.data.items() if 'filter' in k}

    # Get upset if they specify more than a dataset and shapeset filter.
    if len(filters) > 2:
        return api_response.bad_request("Too many table filters provided.")

    # Query the point dataset.
    q = session.query(dataset)

    # If the user specified a geom, filter results to those within its shape.
    if geom:
        q = q.filter(dataset.c.geom.ST_Within(
            sqlalchemy.func.ST_GeomFromGeoJSON(geom)
        ))

    # Retrieve the filters and build conditions from them if they exist.
    point_ctree = filters.get(dataset.name + '__filter')

    # If the user specified point dataset filters, parse and apply them.
    if point_ctree:
        point_conditions = parse_tree(dataset, point_ctree)
        q = q.filter(point_conditions)

        # To allow both obs_date meta params and filter trees.
        q = q.filter(dataset.c.point_date >= obs_date__ge) if obs_date__ge else q
        q = q.filter(dataset.c.point_date <= obs_date__le) if obs_date__le else q

    # If a user specified a shape dataset, it was either through the /shapes
    # enpoint, which uses the aggregate result, or through the /detail endpoint
    # which uses the joined result.
    if shapeset is not None:
        if aggregate:
            q = q.from_self(shapeset).filter(dataset.c.geom.ST_Intersects(shapeset.c.geom)).group_by(shapeset)
        else:
            shape_columns = ['{}.{} as {}'.format(shapeset.name, col.name, col.name) for col in shapeset.c]
            q = q.join(shapeset, dataset.c.geom.ST_Within(shapeset.c.geom))
            q = q.add_columns(*shape_columns)

        # If there's a filter specified for the shape dataset, apply those conditions.
        shape_ctree = filters.get(shapeset.name + '__filter')
        if shape_ctree:
            shape_conditions = parse_tree(shapeset, shape_ctree)
            q = q.filter(shape_conditions)

    return q


def _grid(args):
    meta_params = ('dataset', 'geom', 'resolution', 'buffer', 'obs_date__ge',
                   'obs_date__le')
    meta_vals = (args.data.get(k) for k in meta_params)
    point_table, geom, resolution, buffer_, obs_date__ge, obs_date__le = meta_vals

    result_rows = []

    if not has_tree_filters(args.data):
        tname = point_table.name
        args.data[tname + '__filter'] = request_args_to_condition_tree(
            request_args=args.data,
            ignore=['buffer', 'resolution']
        )

    # We only build conditions from values with a key containing 'filter'.
    # Therefore we only build dataset conditions from condition trees.
    dataset_conditions = {k: v for k, v in args.data.items() if 'filter' in k}
    for tablename, condition_tree in dataset_conditions.items():

        tablename = tablename.split('__')[0]

        metatable = MetaTable.get_by_dataset_name(tablename)
        table = metatable.point_table
        conditions = parse_tree(table, condition_tree)

        try:
            registry_row = MetaTable.get_by_dataset_name(table.name)
            # make_grid expects conditions to be iterable.
            grid_rows, size_x, size_y = registry_row.make_grid(
                resolution,
                geom,
                [conditions],
                {'upper': obs_date__le, 'lower': obs_date__ge}
            )
            result_rows += grid_rows
        except Exception as e:
            msg = 'Could not make grid aggregation.'
            return api_response.make_raw_error("{}: {}".format(msg, e))

    resp = api_response.geojson_response_base()
    for value in result_rows:
        if value[1]:
            pt = shapely.wkb.loads(value[1].decode('hex'))
            south, west = (pt.x - (size_x / 2)), (pt.y - (size_y / 2))
            north, east = (pt.x + (size_x / 2)), (pt.y + (size_y / 2))
            new_geom = shapely.geometry.box(south, west, north, east).__geo_interface__
        else:
            new_geom = None
        new_property = {'count': value[0],}
        api_response.add_geojson_feature(resp, new_geom, new_property)

    return resp


def _meta(args):
    """Generate meta information about table(s) with records from MetaTable.

    :param args: dictionary of request arguments (?foo=bar)

    :returns: response dictionary"""

    meta_params = ('dataset', 'geom', 'obs_date__ge', 'obs_date__le')
    meta_vals = (args.data.get(k) for k in meta_params)
    dataset, geom, start_date, end_date = meta_vals

    # Columns to select as-is
    cols_to_return = ['human_name', 'dataset_name', 'source_url', 'view_url',
                      'date_added', 'last_update', 'update_freq', 'attribution',
                      'description', 'obs_from', 'obs_to', 'column_names']
    col_objects = [getattr(MetaTable, col) for col in cols_to_return]

    # Columns that need pre-processing
    col_objects.append(sqlalchemy.func.ST_AsGeoJSON(MetaTable.bbox))
    cols_to_return.append('bbox')

    # Only return datasets that have been successfully ingested
    q = session.query(*col_objects).filter(MetaTable.date_added.isnot(None))

    # Filter over datasets if user provides full date range or geom
    should_filter = geom or (start_date and end_date)

    if dataset is not None:
        # If the user specified a name, don't try any filtering.
        # Just spit back that dataset's metadata.
        q = q.filter(MetaTable.dataset_name == dataset.name)

    # Otherwise, just send back all the (filtered) datasets
    elif should_filter:
        if geom:
            intersects = sqlalchemy.func.ST_Intersects(
                sqlalchemy.func.ST_GeomFromGeoJSON(geom),
                MetaTable.bbox
            )
            q = q.filter(intersects)
        if start_date and end_date:
            q = q.filter(
                sqlalchemy.and_(
                    MetaTable.obs_from < end_date,
                    MetaTable.obs_to > start_date
                )
            )

    metadata_records = [dict(zip(cols_to_return, row)) for row in q.all()]
    for record in metadata_records:
        try:
            if record.get('bbox') is not None:
                # serialize bounding box geometry to string
                record['bbox'] = json.loads(record['bbox'])
            # format columns in the expected way
            record['columns'] = [{'field_name': k, 'field_type': v}
                                 for k, v in record['column_names'].items()]
        except Exception as e:
            args.warnings.append(e.message)

        # clear column_names off the json, users don't need to see it
        del record['column_names']

    return metadata_records


# =====
# Utils
# =====

def request_args_to_condition_tree(request_args, ignore=list()):
    """Take dictionary that has a 'dataset' key and column arguments into
    a single and build a condition tree.

    :param request_args: dictionary with a dataset and column arguments
    :param ignore: what values to not use for building conditions

    :returns: condition tree"""

    ignored = {'agg', 'data_type', 'dataset', 'geom', 'limit', 'offset',
               'shape', 'shapeset', 'job', 'all', 'datadump_part', 'datadump_total',
               "datadump_requestid", "datadump_urlroot", "jobsframework_ticket", "jobsframework_workerid",
               "jobsframework_workerbirthtime"}
    for val in ignore:
        ignored.add(val)

    # If the key wasn't convertable, it meant that it was a column key.
    columns = {k: v for k, v in request_args.items() if k not in ignored}

    ctree = {"op": "and", "val": []}

    # Add AND conditions based on query string parameters.
    for k, v in columns.items():
        k = k.split('__')
        if k[0] == 'obs_date':
            k[0] = 'point_date'
        if k[0] == 'date' and 'time_of_day' in k[1]:
            k[0] = sqlalchemy.func.date_part('hour', request_args.get('dataset').c.point_date)
            k[1] = 'le' if 'le' in k[1] else 'ge'

        # It made me nervous that you could pass the parser in the validator
        # with values like 2000; or 2000' (because the parser strips them).
        # Before letting those values get passed to psycopg, I'm
        # just going to convert those values to datetimes here.
        elif 'date' in k[0]:
            try:
                v = parser.parse(v)
            except (AttributeError, TypeError):
                pass

        if len(k) == 1:
            ctree['val'].append({"op": "eq", "col": k[0], "val": v})
        elif len(k) == 2:
            ctree['val'].append({"op": k[1], "col": k[0], "val": v})

    return ctree
