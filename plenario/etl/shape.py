# -*- coding: utf-8 -*-

import zipfile
from datetime import datetime

from boto.s3.connection import S3ResponseError

from plenario.database import session
from plenario.etl.common import ETLFile
from plenario.models import ShapeMetadata
from plenario.etl.point import PlenarioETLError
from plenario.utils.shapefile import import_shapefile, ShapefileError


class ShapeETL:

    def __init__(self, meta, source_path=None):
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

        # Get a handle to the shapefile.
        with ETLFile(source_url=self.source_url, source_path=self.source_path) as file_helper:

            # Attempt insertion
            try:
                with zipfile.ZipFile(file_helper.handle) as shapefile_zip:
                    import_shapefile(shapefile_zip=shapefile_zip, table_name=self.table_name)
            except zipfile.BadZipfile:
                raise PlenarioETLError("Source file was not a valid .zip")
            except ShapefileError as e:
                raise PlenarioETLError("Failed to import shapefile.\n{}".format(repr(e)))
