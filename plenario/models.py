from uuid import uuid4
from datetime import datetime

from sqlalchemy import Column, Integer, String, Boolean, Date, DateTime, \
    Text, BigInteger, func
from sqlalchemy.dialects.postgresql import TIMESTAMP, DOUBLE_PRECISION, ARRAY
from geoalchemy2 import Geometry
from sqlalchemy.orm import synonym
from flask_bcrypt import Bcrypt

from plenario.database import session, Base
from plenario.utils.helpers import slugify

bcrypt = Bcrypt()


class MetaTable(Base):
    __tablename__ = 'meta_master'
    dataset_name = Column(String(100), nullable=False)
    human_name = Column(String(255), nullable=False)
    description = Column(Text)
    source_url = Column(String(255))
    source_url_hash = Column(String(32), primary_key=True)
    attribution = Column(String(255))
    obs_from = Column(Date)
    obs_to = Column(Date)
    bbox = Column(Geometry('POLYGON', srid=4326))
    update_freq = Column(String(100), nullable=False)
    last_update = Column(DateTime)
    date_added = Column(DateTime)
    # Store the names of fields in source data
    business_key = Column(String, nullable=False)
    observed_date = Column(String, nullable=False)
    latitude = Column(String)
    longitude = Column(String)
    location = Column(String)
    approved_status = Column(String) # if False, then do not display without first getting administrator approval
    contributor_name = Column(String)
    contributor_organization = Column(String)
    contributor_email = Column(String)
    contributed_data_types = Column(Text) # Temporarily store user-submitted data types for later approval
    is_socrata_source = Column(Boolean, default=False)
    result_ids = Column(ARRAY(String))

    def __repr__(self):
        return '<MetaTable %r (%r)>' % (self.human_name, self.dataset_name)

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class MasterTable(Base):
    __tablename__ = 'dat_master'
    master_row_id = Column(BigInteger, primary_key=True)
    # Looks like start_date and end_date aren't used.
    start_date = Column(TIMESTAMP)
    end_date = Column(TIMESTAMP)
    # current_flag is never updated. We can probably get rid of this
    current_flag = Column(Boolean, default=True)
    location = Column(String(200))
    latitude = Column(DOUBLE_PRECISION(precision=53))
    longitude = Column(DOUBLE_PRECISION(precision=53))
    obs_date = Column(TIMESTAMP, index=True)
    weather_observation_id = Column(BigInteger, index=True)
    census_block = Column(String(15), index=True)
    # Looks like geotag3 is unused
    geotag3 = Column(String(50))
    dataset_name = Column(String(100), index=True)
    dataset_row_id = Column(Integer)
    location_geom = Column(Geometry('POINT', srid=4326))

    def __repr__(self):
        return '<Master %r (%r)>' % (self.dataset_row_id, self.dataset_name)


class ShapeMetadata(Base):
    __tablename__ = 'meta_shape'
    dataset_name = Column(String, primary_key=True)
    human_name = Column(String, nullable=False)
    source_url = Column(String)
    date_added = Column(Date, nullable=False)
    # We always ingest geometric data as 4326
    bbox = Column(Geometry('POLYGON', srid=4326))
    # False when admin first submits metadata.
    # Will become true if ETL completes successfully.
    is_ingested = Column(Boolean, nullable=False)
    # foreign key of celery task responsible for shapefile's ingestion
    celery_task_id = Column(String)

    """
    A note on `caller_session`.
    tasks.py calls on a different scoped_session than the rest of the application.
    To make these functions usable from the tasks and the regular application code,
    we need to pass in a session rather than risk grabbing the wrong session from the registry.
    """

    @classmethod
    def get_all_with_etl_status(cls, caller_session):
        """
        :return: Every row of meta_shape joined with celery task status.
        """
        shape_query = '''
            SELECT meta.*, celery.status
            FROM meta_shape as meta
            LEFT JOIN celery_taskmeta as celery
            ON celery.task_id = meta.celery_task_id;
        '''

        return list(caller_session.execute(shape_query))

    @classmethod
    def index(cls, caller_session):
        result = caller_session.query(cls.dataset_name,
                                        cls.human_name,
                                        cls.date_added,
                                        func.ST_AsGeoJSON(cls.bbox))\
                                 .filter(cls.is_ingested)
        field_names = ['dataset_name', 'human_name', 'date_added', 'bounding_box']
        listing = [dict(zip(field_names, row)) for row in result]
        for dataset in listing:
            dataset['date_added'] = str(dataset['date_added'])
        return listing

    @classmethod
    def get_metadata_with_etl_result(cls, table_name, caller_session):
        query = '''
            SELECT meta.*, celery.status, celery.traceback, celery.date_done
            FROM meta_shape as meta
            LEFT JOIN celery_taskmeta as celery
            ON celery.task_id = meta.celery_task_id
            WHERE meta.dataset_name='{}';
        '''.format(table_name)

        metadata = caller_session.execute(query).first()
        return metadata

    @classmethod
    def get_by_human_name(cls, human_name, caller_session):
        caller_session.query(cls).get(cls.make_table_name(human_name))

    @classmethod
    def make_table_name(cls, human_name):
        return slugify(human_name)

    @classmethod
    def add(cls, caller_session, human_name, source_url):
        table_name = ShapeMetadata.make_table_name(human_name)
        new_shape_dataset = ShapeMetadata(dataset_name=table_name,
                                              human_name=human_name,
                                              is_ingested=False,
                                              source_url=source_url,
                                              date_added=datetime.now().date(),
                                              bbox=None)

        caller_session.add(new_shape_dataset)
        return new_shape_dataset

    def remove_table(self, caller_session):
        if self.is_ingested:
            drop = "DROP TABLE {};".format(self.dataset_name)
            caller_session.execute(drop)
        caller_session.delete(self)

    def update_after_ingest(self, caller_session):
        self.is_ingested = True
        self.bbox = self._make_bbox(caller_session)

    def _make_bbox(self, caller_session):
        bbox_query = 'SELECT ST_Envelope(ST_Union(geom)) FROM {};'.format(self.dataset_name)
        box = caller_session.execute(bbox_query).first().st_envelope
        return box


def get_uuid():
    return unicode(uuid4())

class User(Base):
    __tablename__ = 'plenario_user'
    id = Column(String(36), default=get_uuid, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    email = Column(String, nullable=False)
    _password = Column('password', String(60), nullable=False)

    def _get_password(self):
        return self._password

    def _set_password(self, value):
        self._password = bcrypt.generate_password_hash(value)

    password = property(_get_password, _set_password)
    password = synonym('_password', descriptor=password)

    def __init__(self, name, password, email):
        self.name = name
        self.password = password
        self.email = email

    @classmethod
    def get_by_username(cls, name):
        return session.query(cls).filter(cls.name == name).first()

    @classmethod
    def check_password(cls, name, value):
        user = cls.get_by_username(name)
        if not user:
            return False
        return bcrypt.check_password_hash(user.password, value)

    def is_authenticated(self):
        return True
    
    def is_active(self):
        return True

    def is_anonymous(self):
        return False

    def get_id(self):
        return self.id