# -*- coding: utf-8 -*-

import os
import shutil
import subprocess
from datetime import datetime
import zipfile

import requests
from boto.s3.connection import S3Connection, S3ResponseError
from boto.s3.key import Key
from geoalchemy2.shape import from_shape

from plenario.database import session, task_engine as engine
from plenario.settings import AWS_ACCESS_KEY, AWS_SECRET_KEY, S3_BUCKET, DATA_DIR

from plenario.models import PolygonDataset
import hashlib

from plenario.utils.etl import PlenarioETLError


# There's no consistent naming convention for the components of a shapefile (foo.shp, foo.dbf, etc.)
# Sometimes they'll have the name of the zip they came in, but sometimes not.
# Instead of finding out what the prefix name is and passing it around,
# we'll just rename the components.
SHAPEFILE_COMPONENT_PREFIX = 'component'


class PolygonETL:

    def __init__(self, table_name, save_to_s3=True):
        self.table_name = table_name
        self.save_to_s3 = save_to_s3

    # Aspirational interface
    def import_kml(self, source_url, source_path=None):
        pass

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
        # If source_path is not None, then we can ingest locally instead of hitting the URL
        is_local_ingest = bool(source_path)

        if is_local_ingest:
            shapefile_path = source_path
        else:
            shapefile_path = self._download_shapefile_from_url(source_url)

        # Do we have a valid .zip?
        if not zipfile.is_zipfile(shapefile_path):
            raise PlenarioETLError("We only know how to handle zipped shapefiles.")

        if self.save_to_s3:
            try:
                upload_file_s3(self.table_name, shapefile_path)
            except S3ResponseError as e:
                # If AWS storage fails, soldier on.
                print "Failed to upload file to S3.\n" + e.message

        make_new_polygon_table_from_shapefile(self.table_name, shapefile_path, source_srid, create_mode)
        shapefile_hash = sha_hash_file(shapefile_path)

        # If the file wasn't already on the system, delete it
        if not is_local_ingest:
            os.remove(shapefile_path)

        return shapefile_hash


    def _download_shapefile_from_url(self, source_url):
        # Let the name of the file be the name of the table we're loading it into
        # Assumption: if there's a local name conflict, we want to delete and pull down the latest.
        download_path = os.path.join(DATA_DIR, self.table_name)
        if os.path.exists(download_path):
            os.remove(download_path)

        try:
            download_file_local(source_url, download_path)
        except IOError:
            # If the download fails, delete the incomplete file and stop the ETL process.
            os.remove(download_path)
            raise IOError("Failed to download file from " + source_url)

        return download_path


def make_new_polygon_table_from_shapefile(table_name, shapefile_path, source_srid, create_mode):
    """
    Assumes source_url points to a zipped ESRI shapefile
    :raises: IOError if intermediate file operations fail
             subprocess.CalledProcessError if shp2pgsql fails
    """

    try:
        unzipped_dir_path = unzip_shapefile(shapefile_path)
    except (IOError, OSError) as e:
        # If extraction fails, stop the ETL process.
        print e.message
        raise IOError("Failed to extract Shapefile components from " + shapefile_path)

    try:
        component_path = os.path.join(unzipped_dir_path, SHAPEFILE_COMPONENT_PREFIX)
        import_statement = generate_shapefile_import_statement(component_path, table_name, source_srid, create_mode)
    except subprocess.CalledProcessError as e:
        # External call to shp2pgsql failed.
        raise e
    finally:
        # Either way, clear out the extracted shapefile components
        shutil.rmtree(unzipped_dir_path)

    engine.execute(import_statement)


def sha_hash_file(file_path):
    # Thank you http://pythoncentral.io/hashing-files-with-python/
    sha = hashlib.sha1()
    CHUNK_SIZE = 1024
    with open(file_path, 'rb') as f:
        buf = f.read(CHUNK_SIZE)
        while len(buf) > 0:
            sha.update(buf)
            buf = f.read(CHUNK_SIZE)

    return sha.hexdigest()


def polygon_source_has_changed(table_name, polygon_file_path):
    old_hash = session.query(PolygonDataset.source_hash)\
                      .filter_by(dataset_name=table_name)\
                      .first()\
                      .source_hash

    new_hash = sha_hash_file(polygon_file_path)

    return old_hash != new_hash


def download_file_local(url, download_path):
    """
    Download file to local data directory.
    :param url: url from where shapefile should be downloaded
    :type url: str
    :param download_path: destination on local file system
    :type download_path: str
    :raises: IOError
    """

    # The file might be big, so stream it in chunks.
    file_stream_request = requests.get(url, stream=True, timeout=5)
    # Raise an exception if we didn't get a 200
    file_stream_request.raise_for_status()

    with open(download_path, 'wb') as fd:
        print "opened " + download_path

        # Download and write to disk in 1MB chunks.
        for chunk in file_stream_request.iter_content(chunk_size=1024):
            if chunk:
                fd.write(chunk)
                fd.flush()


def upload_file_s3(table_name, download_path):
    # Use current time to create uniquely named file in S3 bucket
    now_timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    s3_path = '{}/{}.zip'.format(table_name, now_timestamp)

    # Set up the S3 connection
    s3conn = S3Connection(AWS_ACCESS_KEY, AWS_SECRET_KEY)
    bucket = s3conn.get_bucket(S3_BUCKET)
    s3_key = Key(bucket)
    s3_key.key = s3_path

    # Upload to S3
    with open(download_path, 'r') as local_file:
        s3_key.set_contents_from_file(local_file)


def unzip_shapefile(zip_path):
    """
    Unzip shapefile specified by file_name
    and rename its components to file_name.*
    :param zip_path: The name of the zipped directory.
    :raises: IOError, OSError
    """

    # Make a temporary directory to extract to.
    unzipped_dir_path = os.path.join(DATA_DIR, 'temp_unzipped')

    try:
        # If there's a name conflict, overwrite.
        if os.path.exists(unzipped_dir_path):
            shutil.rmtree(unzipped_dir_path)

        # Extract to the new directory
        with zipfile.ZipFile(zip_path, 'r') as shape_dir:
            shape_dir.extractall(unzipped_dir_path)

        # In the new directory,
        # change every component's file prefix to SHAPEFILE_COMPONENT_PREFIX so that we know where to point shp2pgsql.
        for shape_file_name in os.listdir(unzipped_dir_path):
            # Assumption: every filename is formatted like name.suffix
            # Will raise IndexError if assumption is not met.
            # Only split on the first dot to handle oddballs like foo.shp.xml
            file_suffix = str.split(shape_file_name, '.', 1)[1]

            normalized_name = "{}.{}".format(SHAPEFILE_COMPONENT_PREFIX, file_suffix)
            os.rename(os.path.join(unzipped_dir_path, shape_file_name),
                      os.path.join(unzipped_dir_path, normalized_name))

    except (OSError, IndexError, zipfile.BadZipfile) as e:
        # Clean up the temp directory and bail
        shutil.rmtree(unzipped_dir_path)
        raise IOError(e.output + e.message)

    return unzipped_dir_path


def generate_shapefile_import_statement(components_path, target_table, source_srid, create_mode):
    """
    Call out to shp2pgsql to generate import statement.
    :param components_path: Path to point shp2pgsql to. DATA_DIR/table_name/SHAPEFILE_COMPONENT_PREFIX
    :type components_path: str
    :param source_srid: Spatial reference id of shapefile.
    :type source_srid: int
    :return: import_statement
    """

    # (d)rop and recreate, (a)ppend, (c)reate new, (p)repare table without data
    assert create_mode in ['d', 'a', 'c', 'p']

    # We import everything as SRID 4326: lat/long plate carée.
    TARGET_SRID = 4326
    srs_transformation = str(source_srid) + ':' + str(TARGET_SRID)

    # For clarity's sake, specify schema and table.
    TARGET_SCHEMA = 'public'
    table_location = '{}.{}'.format(TARGET_SCHEMA, target_table)

    # Use the postgres shp2pgsql command line utility.
    shp2pgsql_args = ['shp2pgsql',
                      '-' + create_mode,            # (-d|a|c|p)
                      '-s', srs_transformation,     # [<from>:]<srid>
                      '-I',                         # Create a spatial index on the geometry column
                      components_path,              # <shapefile>
                      table_location]               # [[<schema.]<table>]

    try:
        import_statement = subprocess.check_output(shp2pgsql_args)
        # Return the SQL generated by shp2pgsql
        return import_statement

    except subprocess.CalledProcessError as e:
        # Log what happened
        print "Command: \n {} \n failed with return code {} and output: {}".format(e.cmd, str(e.returncode), e.output)
        # And make the caller deal with it
        raise e


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