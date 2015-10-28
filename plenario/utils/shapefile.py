import os
import tempfile
import shutil
from plenario.utils.ogr2ogr import import_shapefile_to_table, OgrError


class ShapefileError(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)
        self.message = message


def import_shapefile(shapefile_zip, table_name):
    """
    :param shapefile_zip: The zipped shapefile.
    :type shapefile_zip: A Python zipfile.ZipFile object
    """

    try:
        with Shapefile(shapefile_zip) as shape:
            shape.insert_in_database(table_name)
    except ShapefileError as e:
        raise e
    except Exception as e:
        raise ShapefileError("Shapefile import failed.\n{}".format(repr(e)))


class Shapefile:
    """
    Encapsulate unzipping and exporting of a Shapefile.
    """
    COMPONENT_PREFIX = 'component'

    def __init__(self, shapefile_zip):
        """
        :param shapefile_zip: The zipped shapefile.
        :type shapefile_zip: A Python zipfile.ZipFile object
        """
        self.shapefile_zip = shapefile_zip

    def __enter__(self):
        """
        Create a temporary directory
        and extract all shapefile components to it as 'COMPONENT_PREFIX.*'.
        Store that directory's path in self.unzip_dir.
        """

        # Extract all shapefile components to a temporary directory.
        # As implemented now, this creates an absolute path traversal vulnerability.
        # https://cwe.mitre.org/data/definitions/36.html
        # Proposed rewrite: just look for the files with the endings you need (.dbf and friends)
        # and use zipfile.extract() to strip path prefixes
        self.unzip_dir = tempfile.mkdtemp()
        self.shapefile_zip.extractall(self.unzip_dir)

        suffixes = []
        # In the new directory,
        # change every component's file prefix to COMPONENT_PREFIX so that we know where to point ogr2ogr.
        for shapefile_name in os.listdir(self.unzip_dir):
            # Assumption: every filename is formatted like name.suffix
            # Will raise IndexError if assumption is not met.
            # Only split on the first dot to handle oddballs like foo.shp.xml
            file_suffix = str.split(shapefile_name, '.', 1)[1]
            suffixes.append(file_suffix)
            normalized_name = "{}.{}".format(Shapefile.COMPONENT_PREFIX, file_suffix)
            os.rename(os.path.join(self.unzip_dir, shapefile_name),
                      os.path.join(self.unzip_dir, normalized_name))

        # Ideally we have a .dbf too, but we can't move on without a shape and a projection.
        if 'shp' not in suffixes or 'prj' not in suffixes:
            raise ShapefileError('Shapefile missing a .shp or .prj component')

        return self

    def insert_in_database(self, table_name):
        component_path = os.path.join(self.unzip_dir, Shapefile.COMPONENT_PREFIX)
        try:
            import_shapefile_to_table(component_path=component_path, table_name=table_name)
        except OgrError as e:
            raise ShapefileError('Failed to insert shapefile into database.\n{}'.format(repr(e)))

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        When a Shapefile exits its managed context or there is an internal exception,
        remove the temporary directory.
        """
        shutil.rmtree(self.unzip_dir)

