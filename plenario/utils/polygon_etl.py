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

from plenario.models import PolygonMetadata
import hashlib

from plenario.utils.etl import PlenarioETLError
from plenario.utils.helpers import slugify


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


class PolygonETL:

    def __init__(self, polygon_table, save_to_s3=False):
        """
        :param PolygonTable:
        """
        self.save_to_s3 = save_to_s3
        self.polygon_table = polygon_table

    def import_shapefile(self, source_srid, source_url, source_path=None):
        if self.polygon_table.exists():
            raise PlenarioETLError("Trying to create table with name that has already been claimed.")

        shapefile_hash = self._ingest_shapefile(source_srid, source_url, source_path, 'c')

        self.polygon_table.add_to_meta(source_url, source_srid)

    def _ingest_shapefile(self, source_srid, source_url, source_path, create_mode):

        def insert_in_database(shapefile_handle):
            # Given a valid shapefile...
            with zipfile.ZipFile(shapefile_handle) as shapefile_zip:
                # that has the expected composition of a shapefile...
                with Shapefile(shapefile_zip, source_srid=source_srid) as shape:
                    # we can generate a big SQL import statement.
                    import_statement = shape.generate_import_statement(self.polygon_table.table_name, create_mode)

            engine.execute(import_statement)

        def attempt_save_to_s3(file_helper):
            try:
                # Use current time to create uniquely named file in S3 bucket
                now_timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
                s3_path = '{}/{}.zip'.format(self.polygon_table.table_name, now_timestamp)
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


class PolygonTable:
    def __init__(self, human_name):
        """
        :param human_name:
        :type human_name: unicode
        """
        self.human_name = human_name
        self.table_name = PolygonTable.make_table_name(human_name)

    def exists(self):
        return engine.has_table(self.table_name)

    @classmethod
    def make_table_name(cls, human_name):
        return slugify(human_name)

    def get_metadata(self):
        return session.query(PolygonMetadata.source_url,
                             PolygonMetadata.source_srid,)\
                      .filter_by(dataset_name=self.table_name)\
                      .first()

    def _make_bbox(self):
        bbox_query = 'SELECT ST_Envelope(ST_Union(geom)) FROM {};'.format(self.table_name)
        box = engine.execute(bbox_query).first().st_envelope
        return box

    def add_to_meta(self, source_url, source_srid):
        """
        Add table_name to meta_polygon
        """

        new_polygon_dataset = PolygonMetadata(dataset_name=self.table_name,
                                              human_name=self.human_name,
                                             source_url=source_url,
                                             date_added=datetime.now().date(),
                                             source_srid=source_srid,
                                             bbox=self._make_bbox())
        try:
            session.add(new_polygon_dataset)
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
