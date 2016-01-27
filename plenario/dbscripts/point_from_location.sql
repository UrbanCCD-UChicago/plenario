
CREATE OR REPLACE FUNCTION point_from_loc(loc text)
    RETURNS geometry(Point,4326)
AS
$$
SELECT ST_PointFromText('POINT(' || subq.longitude || ' ' || subq.latitude || ')', 4326)
FROM ( SELECT FLOAT8((regexp_matches($1, '\((.*),.*\)'))[1]) AS latitude,
              FLOAT8((regexp_matches($1, '\(.*,(.*)\)'))[1]) AS longitude) as subq;
$$
LANGUAGE 'sql' IMMUTABLE;