# -*- coding: utf-8 -*-

import tempfile
from datetime import datetime
import zipfile

import requests
from boto.s3.connection import S3Connection, S3ResponseError
from boto.s3.key import Key

from plenario.database import session, app_engine as engine
from plenario.settings import AWS_ACCESS_KEY, AWS_SECRET_KEY, S3_BUCKET
from plenario.utils.shapefile import import_shapefile, ShapefileError

from plenario.models import ShapeMetadata

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


class ShapeETL:

    def __init__(self, meta, source_path=None, save_to_s3=False):
        self.save_to_s3 = save_to_s3
        self.source_path = source_path
        self.table_name = meta.dataset_name
        self.source_url = meta.source_url
        self.meta = meta

    def _get_metadata(self):
        shape_meta = session.query(ShapeMetadata).get(self.table_name)
        if not shape_meta:
            raise PlenarioETLError("Table {} is not registered in the metadata.".format(self.table_name))
        return shape_meta

    def _refresh_metadata(self):
        pass

    def import_shapefile(self):
        if self.meta.is_ingested:
            raise PlenarioETLError("Table {} has already been ingested.".format(self.table_name))

        # NB: this function is not atomic.
        # update_after_ingest could fail after _ingest_shapefile succeeds, leaving us with inaccurate metadata.
        # If this becomes a problem, we can tweak the ogr2ogr import to return a big SQL string
        # rather than just going ahead and importing the shapefile.
        # Then we could put both operations in the same transaction.

        self._ingest_shapefile()
        self.meta.update_after_ingest(session)

        session.commit()

    def _ingest_shapefile(self):

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
        with ETLFile(source_url=self.source_url, source_path=self.source_path) as file_helper:

            # Try to save to S3 first so that we have a record of what the dataset looked like
            # even if insertion fails.
            if self.save_to_s3:
                attempt_save_to_s3(file_helper)

            # Attempt insertion
            try:
                with zipfile.ZipFile(file_helper.handle) as shapefile_zip:
                    import_shapefile(shapefile_zip=shapefile_zip, table_name=self.table_name)
            except zipfile.BadZipfile:
                raise PlenarioETLError("Source file was not a valid .zip")
            except ShapefileError as e:
                raise PlenarioETLError("Failed to import shapefile.\n{}".format(repr(e)))
