import json

from geoalchemy2 import Geometry
from sqlalchemy import Table, String, Column, ForeignKey, ForeignKeyConstraint
from sqlalchemy import func as sqla_fn, Boolean, BigInteger, DateTime, Float
from sqlalchemy.dialects.postgresql import JSONB, DOUBLE_PRECISION
from sqlalchemy.orm import relationship

from plenario.database import postgres_engine
from plenario.database import postgres_base, postgres_session, redshift_base


sensor_to_node = Table(
    'sensor__sensor_to_node',
    postgres_base.metadata,
    Column('sensor', String, ForeignKey('sensor__sensor_metadata.name')),
    Column('network', String),
    Column('node', String),
    ForeignKeyConstraint(
        ['network', 'node'],
        ['sensor__node_metadata.sensor_network', 'sensor__node_metadata.id']
    )
)

feature_to_network = Table(
    'sensor__feature_to_network',
    postgres_base.metadata,
    Column('feature', String, ForeignKey('sensor__feature_metadata.name')),
    Column('network', String, ForeignKey('sensor__network_metadata.name'))
)


def knn(lng, lat, k, network, sensors):
    """Execute a spatial query to select k nearest neighbors given some point.

    :param lng: (float) longitude
    :param lat: (float) latitude
    :param k: (int) number of results to return
    :returns: (list) of nearest k neighbors"""

    # Convert lng-lat to geojson point
    point = "'" + json.dumps({
        "type": "Point",
        "coordinates": [lng, lat]
    }) + "'"

    # How many to limit the initial bounding box query to
    k_10 = k * 10

    # Based off snippet provided on pg 253 of PostGIS In Action (2nd Edition)
    query = """
    WITH bbox_results AS (
      SELECT
        node,
        location,
        array_agg(sensor) AS sensors,
        (SELECT ST_SetSRID(ST_GeomFromGeoJSON({geojson}), 4326)) AS ref_geom
      FROM
        sensor__node_metadata JOIN sensor__sensor_to_node
        ON id=node
      WHERE
          sensor_network = '{network}'
      GROUP BY
        node,
        location
      ORDER BY
        location <#> (SELECT ST_SetSRID(ST_GeomFromGeoJSON ({geojson}), 4326))
      LIMIT {k_10}
    )

    SELECT
      node,
      RANK() OVER(ORDER BY ST_Distance(location, ref_geom)) AS act_r
    FROM bbox_results
    WHERE
      sensors && '{sensors}'::VARCHAR[]
    ORDER BY act_r
    LIMIT {k};
    """.format(
        geojson=point,
        network=network,
        k=k,
        k_10=k_10,
        sensors="{" + ",".join(sensors) + "}"
    )

    return postgres_engine.execute(query).fetchall()


class NetworkMeta(postgres_base):
    __tablename__ = 'sensor__network_metadata'

    name = Column(String, primary_key=True)
    nodes = relationship('NodeMeta')
    info = Column(JSONB)

    @staticmethod
    def index():
        networks = postgres_session.query(NetworkMeta)
        return [network.name.lower() for network in networks]

    def __repr__(self):
        return '<Network "{}">'.format(self.name)

    def tree(self):
        return {n.id: n.tree() for n in self.nodes}

    def sensors(self) -> set:

        keys = []
        for sensor in self.tree().values():
            keys += sensor

        return set(keys)

    def features(self):

        keys = []
        for sensor in self.tree().values():
            for feature in sensor.values():
                keys += feature.keys()

        return set([k.split(".")[0] for k in keys])


class NodeMeta(postgres_base):
    __tablename__ = 'sensor__node_metadata'

    id = Column(String, primary_key=True)
    sensor_network = Column(String, ForeignKey('sensor__network_metadata.name'), primary_key=True)
    location = Column(Geometry(geometry_type='POINT', srid=4326))
    sensors = relationship('SensorMeta', secondary='sensor__sensor_to_node')
    info = Column(JSONB)

    column_editable_list = ("sensors", "info")

    @staticmethod
    def all(network_name):
        query = NodeMeta.query.filter(NodeMeta.sensor_network == network_name)
        return query.all()

    @staticmethod
    def index(network_name):
        return [node.id for node in NodeMeta.all(network_name)]

    @staticmethod
    def nearest_neighbor_to(lng, lat, network, features):
        sensors = set()
        for feature in features:
            feature = FeatureMeta.query.get(feature)
            sensors = sensors | feature.sensors()

        return knn(
            lng=lng,
            lat=lat,
            network=network,
            sensors=sensors,
            k=10
        )

    @staticmethod
    def within_geojson(network: NetworkMeta, geojson: str):
        geom = sqla_fn.ST_GeomFromGeoJSON(geojson)
        within = NodeMeta.location.ST_Within(geom)
        query = NodeMeta.query.filter(within)
        query = query.filter(NodeMeta.sensor_network == network.name)
        return query

    @staticmethod
    def sensors_from_nodes(nodes):
        sensors_list = []
        for node in nodes:
            sensors_list += node.sensors
        return set(sensors_list)

    def features(self) -> set:
        feature_set = set()
        for feature in self.tree().values():
            feature_set.update(feature.keys())
        return feature_set

    def __repr__(self):
        return '<Node "{}">'.format(self.id)

    def tree(self):
        return {s.name: s.tree() for s in self.sensors}


class SensorMeta(postgres_base):
    __tablename__ = 'sensor__sensor_metadata'

    name = Column(String, primary_key=True)
    observed_properties = Column(JSONB)
    info = Column(JSONB)

    def features(self) -> set:
        """Return the features that this sensor reports on."""

        return {e.split('.')[0] for e in self.tree()}

    def __repr__(self):
        return '<Sensor "{}">'.format(self.name)

    def tree(self):
        return {v: k for k, v in self.observed_properties.items()}


class FeatureMeta(postgres_base):
    __tablename__ = 'sensor__feature_metadata'

    name = Column(String, primary_key=True)
    networks = relationship('NetworkMeta', secondary='sensor__feature_to_network')
    observed_properties = Column(JSONB)

    def types(self):
        """Return a dictionary with the properties mapped to their types."""

        return {e['name']: e['type'] for e in self.observed_properties}

    def sensors(self) -> set:
        """Return the set of sensors that report on this feature."""

        results = set()
        for network in self.networks:
            for node in network.tree().values():
                for sensor, properties in node.items():
                    if self.name in {p.split('.')[0] for p in properties}:
                        results.add(sensor)

        return results


    @staticmethod
    def index(network_name=None):
        features = []
        for node in postgres_session.query(NodeMeta).all():
            if network_name is None or node.sensor_network.lower() == network_name.lower():
                for sensor in node.sensors:
                    for prop in sensor.observed_properties.values():
                        features.append(prop.split('.')[0].lower())
        return list(set(features))

    @staticmethod
    def properties_of(feature):
        query = postgres_session.query(FeatureMeta.observed_properties).filter(
            FeatureMeta.name == feature)
        return [feature + "." + prop["name"] for prop in query.first().observed_properties]

    def mirror(self):
        """Create feature tables in redshift for all the networks associated
        with this feature."""

        for network in self.networks:
            self._mirror(network.name)

    def _mirror(self, network_name: str):
        """Create a feature table in redshift for the specified network."""

        columns = []
        for feature in self.observed_properties:
            column_name = feature['name']
            column_type = database_types[feature['type'].upper()]
            columns.append(Column(column_name, column_type, default=None))

        redshift_table = Table(
            '{}__{}'.format(network_name, self.name),
            redshift_base.metadata,
            Column('node_id', String, primary_key=True),
            Column('datetime', DateTime, primary_key=True),
            Column('meta_id', Float, nullable=False),
            Column('sensor', String, nullable=False),
            *columns,
            redshift_distkey='datetime',
            redshift_sortkey='datetime'
        )

        redshift_table.create()

    def __repr__(self):
        return '<Feature "{}">'.format(self.name)


database_types = {
    'FLOAT': DOUBLE_PRECISION,
    'DOUBLE': DOUBLE_PRECISION,
    'STRING': String,
    'BOOL': Boolean,
    'INT': BigInteger,
    'INTEGER': BigInteger
}
