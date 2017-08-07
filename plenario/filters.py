import sqlalchemy


def intersects(table, geom):
    """Creates an intersect filter given a table and geom value."""

    return sqlalchemy.func.ST_Intersects(
        sqlalchemy.func.ST_GeomFromGeoJSON(geom),
        table.bbox
    )
