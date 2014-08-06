import requests
from cStringIO import StringIO
from csvkit.unicsv import UnicodeCSVReader, UnicodeCSVWriter
from dateutil import parser
from datetime import datetime
from plenario.database import task_session as session, task_engine as engine, \
    Base
from sqlalchemy import Table, Column, String, Date, Integer, Float
from geoalchemy2 import Geometry

class WeatherStationError(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)
        self.message = message

class WeatherStationsETL(object):
    """ 
    Download, transform and create table with info about weather stations
    """

    def __init__(self):
        self.stations_url = \
            'http://www1.ncdc.noaa.gov/pub/data/noaa/ish-history.csv'

    def initialize(self):
        self._extract()
        self._transform()
        self._make_table()
        self._load()

    def _extract(self):
        """ Download CSV of station info from NOAA """
        stations = requests.get(self.stations_url)
        if stations.status_code == 200:
            self.station_raw_info = StringIO(stations.content)
            self.station_raw_info.seek(0)
        else:
            self.station_info = None
            raise WeatherStationError('Unable to fetch station data from NOAA. \
                Recieved a %s HTTP status code' % stations.status_code)

    def _transform(self):
        reader = UnicodeCSVReader(self.station_raw_info)
        header = ['wban_code', 'station_name', 'country', 
                  'state', 'call_sign', 'location', 'elevation', 
                  'begin', 'end']
        reader.next()
        self.clean_station_info = StringIO()
        all_rows = []
        for row in reader:
            if row[1] == '99999':
                continue
            elif row[5] and row[6]:
                row.pop(0)
                row.pop(3)
                lat = row[5].replace('+', '')
                lon = row[6].replace('+', '')
                elev = row[7].replace('+', '')
                begin = parser.parse(row[8]).isoformat()
                end = parser.parse(row[9]).isoformat()
                row[5] = 'SRID=4326;POINT(%s %s)' % ((float(lon) / 1000), (float(lat) / 1000))
                row[6] = float(elev) / 10
                row[7] = begin
                row[8] = end
                row.pop()
                all_rows.append(row)
        writer = UnicodeCSVWriter(self.clean_station_info)
        writer.writerow(header)
        writer.writerows(all_rows)
        self.clean_station_info.seek(0)

    def _make_table(self):
        self.table = Table('weather_stations', Base.metadata,
                Column('wban_code', String(5)),
                Column('station_name', String(50), nullable=False),
                Column('country', String(2), nullable=False),
                Column('state', String(2)),
                Column('call_sign', String(5)),
                Column('location', Geometry('POINT', srid=4326)), 
                Column('elevation', Float),
                Column('begin', Date),
                Column('end', Date))
        self.table.create(engine, checkfirst=True)

    def _load(self):
        names = [c.name for c in self.table.columns]
        ins_st = "COPY weather_stations FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')"
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.copy_expert(ins_st, self.clean_station_info)
        conn.commit()
        return 'bluh'
    
