# -*- coding: utf-8 -*-

import tempfile
from datetime import datetime
import zipfile

import requests
from boto.s3.connection import S3Connection, S3ResponseError
from boto.s3.key import Key

from plenario.database import session, task_engine as engine
from plenario.settings import AWS_ACCESS_KEY, AWS_SECRET_KEY, S3_BUCKET, DATA_DIR
from plenario.utils.shapefile import Shapefile

from plenario.models import PolygonDataset
import hashlib

from plenario.utils.etl import PlenarioETLError


class ETLFile:
    """
    Encapsulates whether a file has been downloaded temporarily
    or is coming from the local file system.
    If initialized with source_path, it opens file on local filesystem.
    If initialized with source_url, it attempts to download file.

    Implements context manager interface with __enter__ and __exit__.
    """
    def __init__(self, source_path=None, source_url=None):
        if source_path and source_url:
            raise RuntimeError('ETLFile takes exactly one of source_path and source_url. Both were given.')

        if not source_path and not source_url:
            raise RuntimeError('ETLFile takes exactly one of source_path and source_url. Neither were given.')

        self.source_path = source_path
        self.source_url = source_url
        self.is_local = bool(source_path)

    def __enter__(self):
        """
        Assigns an open file object to self.file_handle
        """
        if self.is_local:
            self.handle = open(self.source_path, 'r')
        else:
            self._download_temp_file(self.source_url)

        # Return the whole ETLFile so that the `with foo as bar:` syntax looks right.
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # If self.handle is to a file that was already on the file system,
        # .close() acts as we expect.
        # If self.handle is to a TemporaryFile that we downloaded for this purpose,
        # .close() also deletes it from the filesystem.
        self.handle.close()

    def _download_temp_file(self, url):
        """
        Download file to local data directory.
        :param url: url from where file should be downloaded
        :type url: str
        :raises: IOError
        """

        # The file might be big, so stream it in chunks.
        file_stream_request = requests.get(url, stream=True, timeout=5)
        # Raise an exception if we didn't get a 200
        file_stream_request.raise_for_status()

        # Make this temporary file our file handle
        self.handle = tempfile.TemporaryFile()

        # Download and write to disk in 1MB chunks.
        for chunk in file_stream_request.iter_content(chunk_size=1024):
            if chunk:
                self.handle.write(chunk)
                self.handle.flush()

    def _seek_to_start(function):
        """
        In case the client has has done some read()s on self.handle,
        seek to the start of the file before methods that do reads.
        Seek to start again after the method so that the client always sees a fresh file handle.
        """

        def decorator(self, *args, **kwargs):
            self.handle.seek(0)
            to_return = function(self, *args, **kwargs)
            self.handle.seek(0)
            return to_return

        return decorator

    @_seek_to_start
    def upload_to_s3(self, storage_name):
        # Set up the S3 connection
        s3conn = S3Connection(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        bucket = s3conn.get_bucket(S3_BUCKET)
        s3_key = Key(bucket)
        s3_key.key = storage_name

        # Upload to S3
        s3_key.set_contents_from_file(self.handle)

    @_seek_to_start
    def get_sha1_hash(self):
        # Thank you http://pythoncentral.io/hashing-files-with-python/
        sha = hashlib.sha1()
        chunk_size = 1024  # 1024 seems reasonable. No deeper reason.

        buf = self.handle.read(chunk_size)
        while len(buf) > 0:
            sha.update(buf)
            buf = self.handle.read(chunk_size)

        return sha.hexdigest()


class PolygonETL:

    def __init__(self, table_name, save_to_s3=True):
        self.table_name = table_name
        self.save_to_s3 = save_to_s3

    def import_shapefile(self, source_srid, source_url, source_path=None):
        if engine.has_table(self.table_name):
            raise PlenarioETLError("Trying to create table with name that has already been claimed.")

        shapefile_hash = self._ingest_shapefile(source_srid, source_url, source_path, 'c')

        add_polygon_table_to_meta(self.table_name, source_url, source_srid, shapefile_hash)

    def update_polygon_table(self, source_path=None):
        if not engine.has_table(self.table_name):
            raise PlenarioETLError("Trying to update a table that does not exist:" + self.table_name)

        existing_table_meta = session.query(PolygonDataset.source_url,
                                            PolygonDataset.source_srid,)\
                                     .filter_by(dataset_name=self.table_name)\
                                     .first()

        # Problem: ingest can succeed and then metadata update can fail, leaving us with inaccurate (old) metadata
        shapefile_hash = self._ingest_shapefile(existing_table_meta.source_srid,
                                                existing_table_meta.source_url,
                                                source_path,
                                                'd')

        update_polygon_meta(self.table_name, shapefile_hash)

    def _ingest_shapefile(self, source_srid, source_url, source_path, create_mode):

        def insert_in_database(shapefile_handle):
            # Given a valid shapefile...
            with zipfile.ZipFile(shapefile_handle) as shapefile_zip:
                # that has the expected composition of a shapefile...
                with Shapefile(shapefile_zip, source_srid=source_srid) as shape:
                    # we can generate a big SQL import statement.
                    import_statement = shape.generate_import_statement(self.table_name, create_mode)

            engine.execute(import_statement)

        def attempt_save_to_s3(file_helper):
            try:
                # Use current time to create uniquely named file in S3 bucket
                now_timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
                s3_path = '{}/{}.zip'.format(self.table_name, now_timestamp)
                file_helper.upload_to_s3(s3_path)
            except S3ResponseError as e:
                # If AWS storage fails, soldier on.
                print "Failed to upload file to S3.\n" + e.message

        # Get a handle to the shapefile.
        with ETLFile(source_url=source_url, source_path=source_path) as file_helper:

            # Try to save to S3 first so that we have a record of what the dataset looked like
            # even if insertion fails.
            if self.save_to_s3:
                attempt_save_to_s3(file_helper)

            insert_in_database(file_helper.handle)
            # Return the file's hash so that the caller can update metadata accordingly.
            return file_helper.get_sha1_hash()


def polygon_source_has_changed(table_name, polygon_file_path):
    old_hash = session.query(PolygonDataset.source_hash)\
                      .filter_by(dataset_name=table_name)\
                      .first()\
                      .source_hash

    with ETLFile(source_path=polygon_file_path) as file_helper:
        new_hash = file_helper.get_sha1_hash()

    return old_hash != new_hash


def update_polygon_meta(table_name, file_hash):
    bbox = create_polygon_dataset_bounding_box(table_name)
    session.query(PolygonDataset).filter_by(dataset_name=table_name)\
           .update({
                'source_hash': file_hash,
                'last_update': datetime.now(),
                'bbox': bbox},)
    try:
        session.commit()
    except Exception as e:
        session.rollback()
        raise e


def add_polygon_table_to_meta(table_name, source_url, source_srid, source_hash):
    """
    Add table_name to meta_polygon
    """

    bbox = create_polygon_dataset_bounding_box(table_name)
    new_polygon_dataset = PolygonDataset(dataset_name=table_name,
                                         source_url=source_url,
                                         date_added=datetime.now().date(),
                                         last_update=datetime.now(),
                                         source_hash=source_hash,
                                         source_srid=source_srid,
                                         bbox=bbox)
    try:
        session.add(new_polygon_dataset)
        session.commit()
        print "Committed polygon metadata."
    except Exception as e:
        print e.message
        print 'Rolled back polygon metadata.'
        session.rollback()
        # The caller needs to know.
        raise e


def create_polygon_dataset_bounding_box(dataset_name):
    bbox_query = 'SELECT ST_Envelope(ST_Union(geom)) FROM {};'.format(dataset_name)
    try:
        box = engine.execute(bbox_query).first().st_envelope
        return box
    except Exception as e:
        raise e