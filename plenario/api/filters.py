import sqlalchemy


class FilterMaker(object):
    """
    Given dictionary of validated arguments and a sqlalchemy table,
    generate binary conditions on that table restricting time and geography.
    Can also create a postgres-formatted geography for further filtering
    with just a dict of arguments.
    """

    def __init__(self, args, dataset=None):
        """
        :param args: dict mapping arguments to values as taken from a Validator
        :param dataset: table object of particular dataset being queried, if available
        """
        self.args = args
        self.dataset = dataset

    def time_filters(self):
        """
        :return: SQLAlchemy conditions derived from time arguments on :dataset:
        """
        filters = []
        d = self.dataset
        try:
            lower_bound = d.c.point_date >= self.args['obs_date__ge']
            filters.append(lower_bound)
        except KeyError:
            pass

        try:
            upper_bound = d.c.point_date <= self.args['obs_date__le']
            filters.append(upper_bound)
        except KeyError:
            pass

        try:
            start_hour = self.args['date__time_of_day_ge']
            if start_hour != 0:
                lower_bound = sqlalchemy.func.date_part('hour', d.c.point_date).__ge__(start_hour)
                filters.append(lower_bound)
        except KeyError:
            pass

        try:
            end_hour = self.args['date__time_of_day_le']
            if end_hour != 23:
                upper_bound = sqlalchemy.func.date_part('hour', d.c.point_date).__ge__(end_hour)
                filters.append(upper_bound)
        except KeyError:
            pass

        return filters

    def geom_filter(self, geom_str):
        """
        :param geom_str: geoJSON string from Validator ready to throw into postgres
        :return: geographic filter based on location_geom__within and buffer parameters
        """
        # Demeter weeps
        return self.dataset.c.geom.ST_Within(sqlalchemy.func.ST_GeomFromGeoJSON(geom_str))
