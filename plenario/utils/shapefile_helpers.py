# -*- coding: utf-8 -*-

import os
import shutil
import subprocess
from datetime import datetime
import zipfile

import requests
from boto.s3.connection import S3Connection, S3ResponseError
from boto.s3.key import Key

from plenario.database import session, task_engine as engine
from plenario.settings import AWS_ACCESS_KEY, AWS_SECRET_KEY, S3_BUCKET, DATA_DIR

from plenario.models import MetaTable
from hashlib import md5

from plenario.utils.etl import PlenarioETLError


# There's no consistent naming convention for the components of a shapefile (foo.shp, foo.dbf, etc.)
# Sometimes they'll have the name of the zip they came in, but sometimes not.
# Instead of finding out what the prefix name is and passing it around,
# we'll just rename the components.
SHAPEFILE_COMPONENT_PREFIX = 'component'


class PolygonETL:

    def __init__(self, save_to_s3=True):
        # This seems pretty pointless right now.

        self.save_to_s3 = save_to_s3

    def import_shapefile_from_url(self, table_name, source_url, source_srid):
        # Let the name of the file be the name of the table we're loading it into
        # Assumption: if there's a local name conflict, we want to delete and pull down the latest.
        download_path = os.path.join(DATA_DIR, table_name)
        if os.path.exists(download_path):
            os.remove(download_path)

        try:
            download_file_local(source_url, download_path)
        except IOError:
            # If the download fails, delete the incomplete file and stop the ETL process.
            os.remove(download_path)
            raise IOError("Failed to download file from " + source_url)

        if self.save_to_s3:
            try:
                upload_file_s3(table_name, download_path)
            except S3ResponseError as e:
                print "Failed to upload file to S3.\n" + e.message
                # If AWS storage fails, soldier on.

        self.import_shapefile_local(table_name, download_path, source_srid)

    def import_shapefile_local(self, table_name, shapefile_path, source_srid):
        # Does this zipped shapefile really exist?
        assert zipfile.is_zipfile(shapefile_path)

        # Dangerous assumption: shapefile_path is inside of a place where we can write temporary files with abandon
        self.make_new_polygon_table_from_shapefile(table_name, shapefile_path, source_srid)

    def make_new_polygon_table_from_shapefile(self, table_name, shapefile_path, source_srid):
        """
        Assumes source_url points to a zipped ESRI shapefile
        :raises: AssertionError if table already exists
                 IOError if intermediate file operations fail
                 subprocess.CalledProcessError if shp2pgsql fails
        """

        # The API expects datasets to be prefixed with dat_
        table_name = 'dat_' + table_name
        if engine.has_table(table_name):
            raise PlenarioETLError("Trying to create table with name that has already been claimed.")

        # Problem: when this part succeeds and generate_statement fails, we're left an unzipped local directory
        # that this system can't handle.
        try:
            unzipped_dir_path = unzip_shapefile(shapefile_path)
        except (IOError, OSError):
            # If extraction fails, stop the ETL process.
            raise IOError("Failed to extract Shapefile components")

        try:
            component_path = os.path.join(unzipped_dir_path, SHAPEFILE_COMPONENT_PREFIX)
            import_statement = generate_shapefile_import_statement(component_path, table_name, source_srid)
        except subprocess.CalledProcessError as e:
            # External call to shp2pgsql failed.
            raise e
        finally:
            shutil.rmtree(unzipped_dir_path)

        engine.execute(import_statement)

    def update_polygon_table(self):
        pass


def download_file_local(url, download_path):
    """
    Download file to local data directory.
    :param url: url from where shapefile should be downloaded
    :type url: str
    :param file_name: the name you want the file to have in your DATA_DIR
    :type file_name: str
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
            os.removedirs(unzipped_dir_path)

        # Extract to the new directory
        with zipfile.ZipFile(zip_path, 'r') as shape_dir:
            shape_dir.extractall(unzipped_dir_path)

        # In the new directory,
        # change every component's file prefix to SHAPEFILE_COMPONENT_PREFIX so that we know where to point shp2pgsql.
        for shape_file_name in os.listdir(unzipped_dir_path):
            # Assumption: every filename is formatted like name.suffix
            # Only split on the first dot to handle oddballs like foo.shp.xml
            file_suffix = str.split(shape_file_name, '.', 1)[1]

            normalized_name = "{}.{}".format(SHAPEFILE_COMPONENT_PREFIX, file_suffix)
            os.rename(os.path.join(unzipped_dir_path, shape_file_name),
                      os.path.join(unzipped_dir_path, normalized_name))

    except OSError, zipfile.BadZipfile:
        # Clean up the temp directory and bail
        os.remove(unzipped_dir_path)
        raise IOError

    return unzipped_dir_path


def generate_shapefile_import_statement(components_path, target_table, source_srid):
    """
    Call out to shp2pgsql to generate import statement.
    :param components_path: Path to point shp2pgsql to. DATA_DIR/table_name/SHAPEFILE_COMPONENT_PREFIX
    :type components_path: str
    :param source_srid: Spatial reference id of shapefile.
    :type source_srid: int
    :return: import_statement
    """

    # We import everything as SRID 4326: lat/long plate carée.
    TARGET_SRID = 4326
    srs_transformation = str(source_srid) + ':' + str(TARGET_SRID)

    # We want to start by loading the shapefile into a staging schema.
    TARGET_SCHEMA = 'public'
    table_location = '{}.{}'.format(TARGET_SCHEMA, target_table)

    # Use the postgres shp2pgsql command line utility.
    shp2pgsql_args = ['shp2pgsql',
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


def add_polygon_table_to_meta(target_table, source_url, business_key):
    """
    Add target_table to meta_master
    :param target_table: Name of polygon table in public schema.
    """
    print 'inside polygon metadata'

    d = {
        'dataset_name': target_table,
        'human_name': target_table,
        #'attribution': request.form.get('dataset_attribution'),
        #'description': request.form.get('dataset_description'),
        'source_url': source_url,
        'source_url_hash': md5(source_url).hexdigest(),
        'update_freq': 'yearly',
        'business_key': business_key,
        'observed_date': u'Whenever, man',
        #'latitude': latitude,
        #'longitude': longitude,
        #'location': location,
        #'contributor_name': request.form.get('contributor_name'),
        #'contributor_organization': request.form.get('contributor_organization'),
        #'contributor_email': request.form.get('contributor_email'),
        #'contributed_data_types': json.dumps(data_types),
        'approved_status': True,
        'is_socrata_source': False
    }

    # add this to meta_master
    md = MetaTable(**d)
    try:
        session.add(md)
        session.commit()
        print "I think we committed."
    except Exception as e:
        print e.message
        print 'I think we failed.'
        session.rollback()
