import requests
import os
import tarfile
import zipfile
from cStringIO import StringIO
from csvkit.unicsv import UnicodeCSVReader, UnicodeCSVWriter, \
    UnicodeCSVDictReader
from dateutil import parser
from datetime import datetime, date, timedelta
import calendar
from plenario.database import task_session as session, task_engine as engine, \
    Base
from sqlalchemy import Table, Column, String, Date, Integer, Float
from geoalchemy2 import Geometry
DATA_DIR = os.environ['WOPR_DATA_DIR']

class WeatherError(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)
        self.message = message

class WeatherETL(object):
    """ 
    Download, transform and insert weather data into plenario
    """
    def __init__(self, data_dir=DATA_DIR):
        self.base_url = 'http://cdo.ncdc.noaa.gov/qclcd_ascii'
        self.data_dir = data_dir

    def add_dump(self, fname, current=False):
        self.file_type = 'zipfile'
        if fname.endswith('tar.gz'):
            self.file_type = 'tarfile'
        self._extract(fname, current=current)
        getattr(self, '_transform_%s' % self.file_type)()
        #self._load()

    def _extract(self, fname, current=False):
        fpath = os.path.join(self.data_dir, fname)
        self.raw_weather = StringIO()
        if not os.path.exists(fpath):
            url = '%s/%s' % (self.base_url, fname)
            r = requests.get(url, stream=True)
            with open(fpath, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
                        f.flush()
        if self.file_type == 'tarfile':
            with tarfile.open(fpath, 'r') as tar:
                for tarinfo in tar:
                    if tarinfo.name.endswith('hourly.txt'):
                        self.raw_weather.write(tar.extractfile(tarinfo).read())
        else:
            with zipfile.ZipFile(fpath, 'r') as zf:
                for name in zf.namelist():
                    if name.endswith('hourly.txt'):
                        self.raw_weather.write(zf.open(name).read())

    def _transform_tarfile(self):
        return 'bluh'

    def _transform_zipfile(self):
        return 'bluh'

    def _extract_all(self):
        tar_start = datetime(1996, 7, 1, 0, 0)
        tar_end = datetime(2007, 5, 1, 0, 0)
        zip_start = datetime(2007, 5, 1, 0, 0)
        zip_end = datetime.now() + timedelta(days=30)
        tar_filenames = ['%s.tar.gz' % d.strftime('%Y%m') for d in \
            self._date_span(tar_start, tar_end)]
        zip_filenames = ['QCLD%s.zip' % d.strftime('%Y%m') for d in \
            self._date_span(zip_start, zip_end)]
        for fname in tar_filenames:
            self.add_dump(fname)
        for fname in zip_filenames:
            self.add_dump(fname)

    def _date_span(self, start, end):
        delta = timedelta(days=30)
        while (start.year, start.month) != (end.year, end.month):
            yield start
            start = self._add_month(start)
    
    def _add_month(self, sourcedate):
        month = sourcedate.month
        year = sourcedate.year + month / 12
        month = month %12 + 1
        day = min(sourcedate.day, calendar.monthrange(year, month)[1])
        return date(year, month, day)

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

    def update(self):
        self._extract()
        self._transform()
        # Doing this just so self.table is defined
        self._make_table()
        self._update_stations()

    def _extract(self):
        """ Download CSV of station info from NOAA """
        stations = requests.get(self.stations_url)
        if stations.status_code == 200:
            self.station_raw_info = StringIO(stations.content)
            self.station_raw_info.seek(0)
        else:
            self.station_info = None
            raise WeatherError('Unable to fetch station data from NOAA. \
                Recieved a %s HTTP status code' % stations.status_code)

    def _transform(self):
        reader = UnicodeCSVReader(self.station_raw_info)
        header = ['wban_code', 'station_name', 'country', 
                  'state', 'call_sign', 'location', 'elevation', 
                  'begin', 'end']
        reader.next()
        self.clean_station_info = StringIO()
        all_rows = []
        wbans = []
        for row in reader:
            if row[1] == '99999':
                continue
            elif row[1] in wbans:
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
                wbans.append(row[0])
                all_rows.append(row)
        writer = UnicodeCSVWriter(self.clean_station_info)
        writer.writerow(header)
        writer.writerows(all_rows)
        self.clean_station_info.seek(0)

    def _make_table(self):
        self.table = Table('weather_stations', Base.metadata,
                Column('wban_code', String(5), primary_key=True),
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
    
    def _update_stations(self):
        reader = UnicodeCSVDictReader(self.clean_station_info)
        conn = engine.connect()
        for row in reader:
            station = session.query(self.table).filter(self.table.c.wban_code == row['wban_code']).all()
            if not station:
                ins = self.table.insert().values(**row)
                conn.execute(ins)
