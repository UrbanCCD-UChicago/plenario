from datetime import datetime
from flask_bcrypt import Bcrypt
from geoalchemy2 import Geometry
from sqlalchemy import Column, String, Boolean, Date, Text
from sqlalchemy import Table, select, Integer, func
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.types import NullType

from plenario.database import postgres_session, postgres_base
from plenario.utils.helpers import slugify

bcrypt = Bcrypt()


class ShapeMetadata(postgres_base):
    __tablename__ = 'meta_shape'
    dataset_name = Column(String, primary_key=True)
    human_name = Column(String, nullable=False)
    source_url = Column(String)
    view_url = Column(String)
    date_added = Column(Date, nullable=False)

    # Organization that published this dataset
    attribution = Column(String)
    description = Column(Text)
    update_freq = Column(String(100), nullable=False)

    # Who submitted this dataset?
    contributor_name = Column(String)
    contributor_organization = Column(String)
    contributor_email = Column(String)

    # Has an admin signed off on it?
    approved_status = Column(Boolean)

    # We always ingest geometric data as 4326
    bbox = Column(Geometry('POLYGON', srid=4326))
    # How many shape records are present?
    num_shapes = Column(Integer)
    # False when admin first submits metadata.
    # Will become true if ETL completes successfully.
    is_ingested = Column(Boolean, nullable=False)
    # foreign key of celery task responsible for shapefile's ingestion
    celery_task_id = Column(String)

    @classmethod
    def get_by_dataset_name(cls, name):
        shape_metatable = postgres_session.query(cls).filter(cls.dataset_name == name).first()
        return shape_metatable

    @classmethod
    def get_all_with_etl_status(cls):
        """
        :return: Every row of meta_shape joined with celery task status.
        """
        shape_query = '''
            SELECT meta.*, c.*
            FROM meta_shape as meta
            LEFT JOIN celery_taskmeta as c
            ON c.task_id = meta.celery_task_id
            WHERE meta.approved_status = TRUE;
        '''

        return list(postgres_session.execute(shape_query))

    @classmethod
    def index(cls, geom=None):
        # The attributes that we want to pass along as-is
        as_is_attr_names = ['dataset_name', 'human_name', 'date_added',
                            'attribution', 'description', 'update_freq',
                            'view_url', 'source_url', 'num_shapes']

        as_is_attrs = [getattr(cls, name) for name in as_is_attr_names]

        # We need to apply some processing to the bounding box
        bbox = func.ST_AsGeoJSON(cls.bbox)
        attr_names = as_is_attr_names + ['bbox']
        attrs = as_is_attrs + [bbox]

        result = postgres_session.query(*attrs).filter(cls.is_ingested)
        listing = [dict(list(zip(attr_names, row))) for row in result]

        for dataset in listing:
            dataset['date_added'] = str(dataset['date_added'])

        if geom:
            listing = cls.add_intersections_to_index(listing, geom)

        listing = cls._add_fields_to_index(listing)

        return listing

    @classmethod
    def _add_fields_to_index(cls, listing):
        for dataset in listing:
            name = dataset['dataset_name']
            try:
                # Reflect up the shape table
                table = Table(name,
                              postgres_base.metadata,
                              autoload=True,
                              extend_existing=True)
            except NoSuchTableError:
                # If that table doesn't exist (?!?!)
                # don't try to form the fields.
                continue

            finally:
                # Extract every column's info.
                fields_list = []
                for col in table.columns:
                    if not isinstance(col.type, NullType):
                        # Don't report our internal-use columns
                        if col.name in {'geom', 'ogc_fid', 'hash'}:
                            continue
                        field_object = {
                            'field_name': col.name,
                            'field_type': str(col.type)
                        }
                        fields_list.append(field_object)
                dataset['columns'] = fields_list
        return listing

    @classmethod
    def tablenames(cls):
        return [x.dataset_name for x in postgres_session.query(ShapeMetadata.dataset_name).all()]

    @staticmethod
    def add_intersections_to_index(listing, geom):
        # For each dataset_name in the listing,
        # get a count of intersections
        # and replace num_geoms

        for row in listing:
            name = row['dataset_name']
            num_intersections_query = '''
            SELECT count(g.geom) as num_geoms
            FROM "{dataset_name}" as g
            WHERE ST_Intersects(g.geom, ST_GeomFromGeoJSON('{geojson_fragment}'))
            '''.format(dataset_name=name, geojson_fragment=geom)

            num_intersections = postgres_session.execute(num_intersections_query) \
                .first().num_geoms
            row['num_shapes'] = num_intersections

        intersecting_rows = [row for row in listing if row['num_shapes'] > 0]
        return intersecting_rows

    @classmethod
    def get_metadata_with_etl_result(cls, table_name):
        query = '''
            SELECT meta.*, celery.status, celery.traceback, celery.date_done
            FROM meta_shape as meta
            LEFT JOIN celery_taskmeta as celery
            ON celery.task_id = meta.celery_task_id
            WHERE meta.dataset_name='{}';
        '''.format(table_name)

        metadata = postgres_session.execute(query).first()
        return metadata

    @classmethod
    def get_by_human_name(cls, human_name):
        return postgres_session.query(cls).get(cls.make_table_name(human_name))

    @classmethod
    def make_table_name(cls, human_name):
        return slugify(human_name)

    @classmethod
    def add(cls, human_name, source_url, approved_status, **kwargs):
        table_name = ShapeMetadata.make_table_name(human_name)
        new_shape_dataset = ShapeMetadata(
            # Required params
            dataset_name=table_name,
            human_name=human_name,
            source_url=source_url,
            approved_status=approved_status,
            # Params that reflect just-submitted, not yet ingested status.
            is_ingested=False,
            bbox=None,
            num_shapes=None,
            date_added=datetime.now().date(),
            # The rest
            **kwargs)
        postgres_session.add(new_shape_dataset)
        return new_shape_dataset

    @property
    def shape_table(self):
        try:
            return self._shape_table
        except AttributeError:
            self._shape_table = Table(self.dataset_name, postgres_base.metadata,
                                      autoload=True, extend_existing=True)
            return self._shape_table

    def remove_table(self):
        if self.is_ingested:
            drop = "DROP TABLE {};".format(self.dataset_name)
            postgres_session.execute(drop)
        postgres_session.delete(self)

    def update_after_ingest(self):
        self.is_ingested = True
        self.bbox = self._make_bbox()
        self.num_shapes = self._get_num_shapes()

    def _make_bbox(self):
        bbox_query = 'SELECT ST_Envelope(ST_Union(geom)) FROM {};'. \
            format(self.dataset_name)
        box = postgres_session.execute(bbox_query).first().st_envelope
        return box

    def _get_num_shapes(self):
        table = self.shape_table
        # Arbitrarily select the first column of the table to count against
        count_query = select([func.count(table.c.geom)])
        # Should return only one row.
        # And we want the 0th and only attribute of that row (the count).
        return postgres_session.execute(count_query).fetchone()[0]
