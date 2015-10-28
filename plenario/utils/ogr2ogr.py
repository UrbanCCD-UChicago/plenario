from plenario.settings import DB_PORT, DB_PASSWORD, DB_USER, DB_NAME, DB_HOST
import subprocess
import zipfile
import os
import tempfile
import shutil


postgres_connection_arg = 'PG:host={} user={} port={} dbname={} password={}'.format(
                                  DB_HOST,
                                  DB_USER,
                                  DB_PORT,
                                  DB_NAME,
                                  DB_PASSWORD)


class OgrError(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)
        self.message = message


class OgrExport(object):
    """
    Given a path where we want to export a shape,
    write a file representation of the shape dataset at table_name to that path in the requested format.
    There must not already be a file at the path.
    It is the caller's responsibility to clean up the file we create there.
    """

    def __init__(self, export_format, export_path, table_name):
        self.ogr_format = self._requested_format_to_ogr_format_name(export_format)
        self.table_name = table_name
        self.flags = self._make_flags()
        self.export_path = export_path

    def write_file(self):
        if self.ogr_format == 'ESRI Shapefile':
            self._write_shapefile()
        else:
            self._write_flat_file()

    def _write_shapefile(self):
        temp_dir = tempfile.mkdtemp()
        try:
            self._call_ogr2ogr(temp_dir)
            self._zip_flat_directory(temp_dir)
        finally:
            shutil.rmtree(temp_dir)

    def _zip_flat_directory(self, dir_path):
        # Zip all the files in dir_path and add them to an archive at self.export_path.
        with zipfile.ZipFile(self.export_path, 'w') as zipped:
            for file_name in os.listdir(dir_path):
                path = os.path.join(dir_path, file_name)
                zipped.write(path, file_name)

    def _write_flat_file(self):
        self._call_ogr2ogr(self.export_path)

    def _call_ogr2ogr(self, export_path):
        args = ['ogr2ogr'] + self.flags + [export_path, postgres_connection_arg, self.table_name]
        try:
            subprocess.check_call(args)
        except subprocess.CalledProcessError as e:
            shutil.rmtree(export_path)
            print 'Failed to export dataset to file with ogr2ogr.' + str(args)
            raise OgrError(e.message)

    def _make_flags(self):
        flags = ['-f', self.ogr_format]
        if self.ogr_format == 'ESRI Shapefile':
            flags = flags + ['-lco', 'RESIZE=YES']
        return flags

    @staticmethod
    def _requested_format_to_ogr_format_name(requested_format):
        """
        :param requested_format: Lowercase unicode: one of 'json', 'kml', 'shapefile'
                                 json is default value if request is none of the expected.
        """

        format_map = {
            'json': 'GeoJSON',
            'kml': 'KML',
            'shapefile': 'ESRI Shapefile'
        }

        try:
            ogr_format_name = format_map[requested_format]
        except KeyError:
            ogr_format_name = format_map['json']

        return ogr_format_name


def import_shapefile_to_table(component_path, table_name):
    """

    :param component_path: Path to unzipped shapefile components and the shared name of all components.
                           So if folder contains foo.shp, foo.prj, foo.dbf, then component_path is path/to/dir/foo.
                           foo.shp and foo.prj must be present.
    :param table_name: Name that we want table to have in the database
    """

    args = ['ogr2ogr',
            '-f', 'PostgreSQL',                 # Use the PostgreSQL driver. Documentation here: http://www.gdal.org/drv_pg.html

            '-lco', 'PRECISION=no',             # Many .dbf files don't obey their precision headers.
                                                # So importing as precision-marked types like NUMERIC(width, precision) often fails.
                                                # Instead, import as INTEGER, VARCHAR, FLOAT8.

            '-nlt', 'PROMOTE_TO_MULTI',         # Import all lines and polygons as multilines and multipolygons
                                                # We don't know if the source shapefiles will have multi or non-multi geometries,
                                                # so we need to import the most inclusive set of types.
            '-s_srs', component_path + '.prj',  # Derive source SRID from Well Known Text in .prj
            '-t_srs', 'EPSG:4326',              # Always convert to 4326
            postgres_connection_arg,
            component_path + '.shp',            # Point to .shp so that ogr2ogr knows it's importing a Shapefile.
            '-nln', table_name,                 # (n)ew (l)ayer (n)ame. Set the name of the new table.
            '-lco', 'GEOMETRY_NAME=geom']       # Always name the geometry column 'geom'
    try:
        subprocess.check_call(args)
    except subprocess.CalledProcessError as e:
        print 'Failed to import dataset to postgres with ogr2ogr.' + str(args)
        raise OgrError(e.message)
