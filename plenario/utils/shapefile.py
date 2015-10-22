import os
import tempfile
import subprocess
import shutil


class Shapefile:
    """
    Encapsulate unzipping and exporting of a Shapefile.
    """
    COMPONENT_PREFIX = 'component'

    def __init__(self, shapefile_zip, source_srid=None):
        """
        :param shapefile_zip: The zipped shapefile.
        :type shapefile_zip: A Python zipfile.ZipFile object
        :param source_srid: SRS identifier of shape data. Can be derived from .prj.
        :type source_srid: int
        """
        self.shapefile_zip = shapefile_zip
        self.srid = source_srid

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

        # In the new directory,
        # change every component's file prefix to COMPONENT_PREFIX so that we know where to point shp2pgsql.
        for shapefile_name in os.listdir(self.unzip_dir):
            # Assumption: every filename is formatted like name.suffix
            # Will raise IndexError if assumption is not met.
            # Only split on the first dot to handle oddballs like foo.shp.xml
            file_suffix = str.split(shapefile_name, '.', 1)[1]
            normalized_name = "{}.{}".format(Shapefile.COMPONENT_PREFIX, file_suffix)
            os.rename(os.path.join(self.unzip_dir, shapefile_name),
                      os.path.join(self.unzip_dir, normalized_name))

        return self

    # TODO: Consider using ogr2ogr instead to remove the dependency on shp2pgsql
    # http://www.gdal.org/drv_pgdump.html
    def generate_import_statement(self, table_name, create_mode, target_srid=4326):
        """
        Call out to shp2pgsql to generate import statement.
        :param target_srid: Spatial reference id that spatial data will be encoded as in import statement.
        :type target_srid: int
        :param table_name: Table that shapefile should be imported into.
                           Can be qualified with schema name.
                           Without schema name, will import to 'public' by default.
        :type table_name: str
        :return: import_statement
        """

        # create_mode must be one of four characters:
        # (d)rop and recreate, (a)ppend, (c)reate new, (p)repare table without data
        assert create_mode in ['d', 'a', 'c', 'p']

        # shp2pgsql handles srs transformations if specified as source:target
        srs_transformation = str(self.srid) + ':' + str(target_srid)

        # shp2pgsql expects to be directed to directory/common_prefix_of_shapefile_components
        components_path = os.path.join(self.unzip_dir, Shapefile.COMPONENT_PREFIX)

        # Use the postgres shp2pgsql command line utility.
        shp2pgsql_args = ['shp2pgsql',
                          '-' + create_mode,            # (-d|a|c|p)
                          '-s', srs_transformation,     # [<from>:]<srid>
                          '-I',                         # Create a spatial index on the geometry column
                          components_path,              # <shapefile>
                          table_name]                   # [[<schema.]<table>]

        try:
            import_statement = subprocess.check_output(shp2pgsql_args)
            return import_statement

        except subprocess.CalledProcessError as e:
            # Log what happened
            print "Command: \n {} \n failed.".format(str(shp2pgsql_args))
            # And make the caller deal with it
            raise e

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        When a Shapefile exits its managed context or there is an internal exception,
        remove the temporary directory.
        """
        shutil.rmtree(self.unzip_dir)

