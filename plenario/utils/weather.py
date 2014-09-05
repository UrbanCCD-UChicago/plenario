import requests
import os
import sys
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
from sqlalchemy import Table, Column, String, Date, DateTime, Integer, Float, \
    VARCHAR, BigInteger, UniqueConstraint, and_, select
from sqlalchemy.dialects.postgresql import ARRAY
from geoalchemy2 import Geometry
from uuid import uuid4
DATA_DIR = os.environ['WOPR_DATA_DIR']

class WeatherError(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)
        self.message = message

class WeatherETL(object):
    """ 
    Download, transform and insert weather data into plenario
    """
    weather_type_dict = {'+FC': 'TORNADO/WATERSPOUT',
                     'FC': 'FUNNEL CLOUD',
                     'TS': 'THUNDERSTORM',
                     'GR': 'HAIL',
                     'RA': 'RAIN',
                     'DZ': 'DRIZZLE',
                     'SN': 'SNOW',
                     'SG': 'SNOW GRAINS',
                     'GS': 'SMALL HAIL &/OR SNOW PELLETS',
                     'PL': 'ICE PELLETS',
                     'IC': 'ICE CRYSTALS',
                     'FG': 'FOG', # 'FG+': 'HEAVY FOG (FG & LE.25 MILES VISIBILITY)',
                     'BR': 'MIST',
                     'UP': 'UNKNOWN PRECIPITATION',
                     'HZ': 'HAZE',
                     'FU': 'SMOKE',
                     'VA': 'VOLCANIC ASH',
                     'DU': 'WIDESPREAD DUST',
                     'DS': 'DUSTSTORM',
                     'PO': 'SAND/DUST WHIRLS',
                     'SA': 'SAND',
                     'SS': 'SANDSTORM',
                     'PY': 'SPRAY',
                     'SQ': 'SQUALL',
                     'DR': 'LOW DRIFTING',
                     'SH': 'SHOWER',
                     'FZ': 'FREEZING',
                     'MI': 'SHALLOW',
                     'PR': 'PARTIAL',
                     'BC': 'PATCHES',
                     'BL': 'BLOWING',
                     'VC': 'VICINITY'
                     # Prefixes:
                     # - LIGHT
                     # + HEAVY
                     # "NO SIGN" MODERATE
                 }

    def __init__(self, data_dir=DATA_DIR, debug=False):
        self.base_url = 'http://cdo.ncdc.noaa.gov/qclcd_ascii'
        self.data_dir = data_dir
        self.debug_outfile = sys.stdout
        self.debug = debug
        if (self.debug == True):
            self.debug_outfile = open(os.path.join(self.data_dir, 'weather_etl_debug_out.txt'), 'w')

    # WeatherETL.initialize_last(): for debugging purposes, only initialize the most recent month of weather data.
    def initialize_last(self, start_line=0, end_line=None):
        self.make_tables()
        fname = self._extract_last_fname()
        raw_hourly, raw_daily, file_type = self._extract(fname)
        t_daily = self._transform_daily(raw_daily, file_type, start_line=start_line, end_line=end_line)
        self._load_daily(t_daily)
        t_hourly = self._transform_hourly(raw_hourly, file_type, start_line=start_line, end_line=end_line)
        self._load_hourly(t_hourly)

    def initialize(self): 
        self.make_tables()
        fnames = self._extract_fnames()
        for fname in fnames:
            if (self.debug==True):
                print "INITIALIZE: doing fname", fname
            self._do_etl(fname)

    def initialize_month(self, year, month, no_daily=False, no_hourly=False):
        self.make_tables()
        fname = self._extract_fname(year,month)
        self._do_etl(fname, no_daily, no_hourly)
        
    def _do_etl(self, fname, no_daily=False, no_hourly=False):
        raw_hourly, raw_daily, file_type = self._extract(fname)
        if (not no_daily):
            t_daily = self._transform_daily(raw_daily, file_type)
        if (not no_hourly):
            t_hourly = self._transform_hourly(raw_hourly, file_type)             # this returns a StringIO with all the transformed data
        if (not no_daily):
            self._load_daily(t_daily)                          # this actually imports the transformed StringIO csv
            self._update(span='daily')
        if (not no_hourly):
            self._load_hourly(t_hourly)    # this actually imports the transformed StringIO csv
            self._update(span='hourly')
        self._cleanup_temp_tables()

    def _cleanup_temp_tables(self):
        for span in ['daily', 'hourly']:
            for tname in ['src', 'new']:
                try:
                    table = getattr(self, '%s_%s_table' % (tname, span))
                    table.drop(engine, checkfirst=True)
                except AttributeError:
                    continue

    def _update(self, span=None):
        new_table = Table('new_weather_observations_%s' % span, Base.metadata,
                          Column('wban_code', String(5)), extend_existing=True)
        dat_table = getattr(self, '%s_table' % span)
        src_table = getattr(self, 'src_%s_table' % span)
        from_sel_cols = ['wban_code']
        if span == 'daily':
            from_sel_cols.append('date')
            src_date_col = src_table.c.date
            dat_date_col = dat_table.c.date
            new_table.append_column(Column('date', Date))
            new_date_col = new_table.c.date
        elif span == 'hourly':
            from_sel_cols.append('datetime')
            src_date_col = src_table.c.datetime
            dat_date_col = dat_table.c.datetime
            new_table.append_column(Column('datetime', DateTime))
            new_date_col = new_table.c.datetime
        new_table.drop(engine, checkfirst=True)
        new_table.create(engine)
        ins = new_table.insert()\
                .from_select(from_sel_cols, 
                    select([src_table.c.wban_code, src_date_col])\
                        .select_from(src_table.join(dat_table,
                            and_(src_table.c.wban_code == dat_table.c.wban_code,
                                 src_date_col == dat_date_col),
                            isouter=True)
                    ).where(dat_table.c.id == None)
                )
        conn = engine.contextual_connect()
        try:
            conn.execute(ins)
            new = True
        except TypeError:
            new = False
        if new:
            ins = dat_table.insert()\
                    .from_select([c for c in dat_table.columns if c.name != 'id'], 
                        select([c for c in src_table.columns])\
                            .select_from(src_table.join(new_table,
                                and_(src_table.c.wban_code == new_table.c.wban_code,
                                     src_date_col == new_date_col)
                            ))
                    )
            conn.execute(ins)

    def _transform_hourly(self, raw, file_type, start_line=0, end_line=None):
        t = getattr(self, '_transform_%s_hourly' % file_type)(raw, start_line, end_line)
        return t

    def _transform_daily(self, raw, file_type, start_line=0, end_line=None):
        t = getattr(self, '_transform_%s_daily' % file_type)(raw, start_line, end_line)
        return t
            
    def make_tables(self):
        self._make_daily_table()
        self._make_hourly_table()

    def _extract(self, fname):
        file_type = 'zipfile'
        if fname.endswith('tar.gz'):
            file_type = 'tarfile'
        fpath = os.path.join(self.data_dir, fname)
        raw_weather_hourly = StringIO()
        raw_weather_daily = StringIO()
        if not os.path.exists(fpath):
            url = '%s/%s' % (self.base_url, fname)
            if (self.debug==True):
                self.debug_outfile.write("Extracting: %s\n" % url)
            r = requests.get(url, stream=True)
            with open(fpath, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
                        f.flush()
            f.close() # Explicitly close before re-opening to read.
        if file_type == 'tarfile':
            with tarfile.open(fpath, 'r') as tar:
                for tarinfo in tar:
                    if tarinfo.name.endswith('hourly.txt'):
                        raw_weather_hourly.write(tar.extractfile(tarinfo).read())
                    elif tarinfo.name.endswith('daily.txt'):
                        raw_weather_daily.write(tar.extractfile(tarinfo).read())
        else:
            if (self.debug==True):
                self.debug_outfile.write("extract: fpath is %s\n" % fpath)
            with zipfile.ZipFile(fpath, 'r') as zf:
                for name in zf.namelist():
                    if name.endswith('hourly.txt'):
                        raw_weather_hourly.write(zf.open(name).read())
                    elif name.endswith('daily.txt'):
                        raw_weather_daily.write(zf.open(name).read())
        return raw_weather_hourly, raw_weather_daily, file_type

    def _transform_zipfile_daily(self, raw_weather, start_line=0, end_line=None):
        station_table = Table('weather_stations', Base.metadata, autoload=True, autoload_with=engine)
        wban_list = session.query(station_table.c.wban_code.distinct()). \
                    order_by(station_table.c.wban_code).all()
        
        raw_weather.seek(0)
        reader = UnicodeCSVReader(raw_weather)
        header = reader.next()

        self.clean_observations_daily = StringIO()
        writer = UnicodeCSVWriter(self.clean_observations_daily)
        out_header = ["wban_code","date","temp_max","temp_min",
                      "temp_avg","departure_from_normal",
                      "dewpoint_avg", "wetbulb_avg","weather_types",
                      "snowfall","precip_total", "station_pressure",
                      "sealevel_pressure", 
                      "resultant_windspeed", "resultant_winddirection", "resultant_winddirection_cardinal",
                      "avg_windspeed",
                      "max5_windspeed", "max5_winddirection","max5_winddirection_cardinal",
                      "max2_windspeed", "max2_winddirection","max2_winddirection_cardinal"]
        writer.writerow(out_header)

        row_count = 0
        for row in reader:
            if (row_count % 100 == 0):
                if (self.debug == True):
                    self.debug_outfile.write("\rdaily parsing: row_count=%06d" % row_count)
                    self.debug_outfile.flush()

            if (start_line > row_count):
                row_count +=1
                continue
            if ((end_line is not None) and (row_count > end_line)):
                break

            row_count += 1
            #print zip(header,row)

            wban_code = row[header.index('WBAN')]
            date = row[header.index('YearMonthDay')] # e.g. 20140801
            temp_max = self.floatOrNA(row[header.index('Tmax')])
            temp_min = self.floatOrNA(row[header.index('Tmin')])
            temp_avg = self.floatOrNA(row[header.index('Tavg')])
            departure_from_normal = self.floatOrNA(row[header.index('Depart')])
            dewpoint_avg = self.floatOrNA(row[header.index('DewPoint')])
            wetbulb_avg = self.floatOrNA(row[header.index('WetBulb')])
            weather_types = row[header.index('CodeSum')]
            if (weather_types.strip() == ''):
                weather_types_list = None
            else:
                weather_types_list = weather_types.split(' ')
                weather_types_list = self.list_to_postgres_array(weather_types_list)
            snowfall = self.getPrecip(row[header.index('SnowFall')])
            precip_total= self.getPrecip(row[header.index('PrecipTotal')])
            station_pressure=self.floatOrNA(row[header.index('StnPressure')])
            sealevel_pressure=self.floatOrNA(row[header.index('SeaLevel')])
            resultant_windspeed = self.floatOrNA(row[header.index('ResultSpeed')])
            resultant_winddirection, resultant_winddirection_cardinal=self.getWind(resultant_windspeed, row[header.index('ResultDir')])
            avg_windspeed=self.floatOrNA(row[header.index('AvgSpeed')])            
            max5_windspeed=self.floatOrNA(row[header.index('Max5Speed')])
            max5_winddirection, max5_winddirection_cardinal=self.getWind(max5_windspeed, row[header.index('Max5Dir')])
            max2_windspeed=self.floatOrNA(row[header.index('Max2Speed')])
            max2_winddirection, max2_winddirection_cardinal=self.getWind(max2_windspeed, row[header.index('Max2Dir')])

            writer.writerow([wban_code,date,temp_max,temp_min,
                      temp_avg,departure_from_normal,
                      dewpoint_avg, wetbulb_avg,weather_types_list,
                      snowfall,precip_total, station_pressure,
                      sealevel_pressure, 
                      resultant_windspeed, resultant_winddirection, resultant_winddirection_cardinal,
                      avg_windspeed,
                      max5_windspeed, max5_winddirection,max5_winddirection_cardinal,
                      max2_windspeed, max2_winddirection, max2_winddirection_cardinal])
        return self.clean_observations_daily
        
    def _transform_tarfile_hourly(self, raw_weather, start_line = 0, end_line=None):
        # XX: _transform_tarfile_hourly and _transform_zipfile_hourly should really just be one function that takes 
        # a file_type parameter instead..
        pass
        
    

    def _transform_zipfile_hourly(self, raw_weather, start_line = 0, end_line=None):
        #station_table = Table('weather_stations', Base.metadata, autoload=True, autoload_with=engine)
        #wban_list = session.query(station_table.c.wban_code.distinct()). \
        #            order_by(station_table.c.wban_code).all()
        #station_observations = Table('weather_observations_hourly', Base.metadata, autoload=True, autoload_with=engine)

        raw_weather.seek(0)
        reader = UnicodeCSVReader(raw_weather)
        header= reader.next()

        self.clean_observations_hourly_info = StringIO()
        writer = UnicodeCSVWriter(self.clean_observations_hourly_info)
        out_header = ["wban_code","datetime","old_station_type","station_type", \
                      "sky_condition","sky_condition_top","visibility",\
                      "weather_type","drybulb_fahrenheit","wetbulb_fahrenheit",\
                      "dewpoint_fahrenheit","relative_humidity",\
                      "wind_speed","wind_direction","wind_cardinal",\
                      "station_pressure","sealevel_pressure","record_type",\
                      "hourly_precip"]
        writer.writerow(out_header)

        row_count = 0
        for row in reader:
            if (row_count % 1000 == 0):
                if (self.debug==True):
                    self.debug_outfile.write( "\rparsing: row_count=%06d" % row_count)
                    self.debug_outfile.flush()

            if (start_line > row_count):
                row_count +=1
                continue
            if ((end_line is not None) and (row_count > end_line)):
                break

            row_count += 1

            wban_code = row[header.index('WBAN')]
            date = row[header.index('Date')] # e.g. 20140801
            time = row[header.index('Time')] # e.g. '0601' 6:01am
            weather_date = datetime.strptime('%s %s' % (date, time), '%Y%m%d %H%M')
            station_type = row[header.index('StationType')]
            old_station_type = None
            sky_condition = row[header.index('SkyCondition')]
            # Take the topmost atmospheric observation of clouds (e.g. in 'SCT013 BKN021 OVC029'
            # (scattered at 1300 feet, broken clouds at 2100 feet, overcast at 2900)
            # take OVC29 as the top layer.
            sky_condition_top = sky_condition.split(' ')[-1]
            visibility = self.floatOrNA(row[header.index('Visibility')])
            visibility_flag = row[header.index('VisibilityFlag')]
            # XX mcc consider handling visibility_flag =='s' for 'suspect'
            weather_types = row[header.index('WeatherType')]
            weather_types_flag = row[header.index('WeatherTypeFlag')]
            # XX mcc consider handling weather_type_flag =='s' for 'suspect'
            if (weather_types.strip() == ''):
                weather_types_list = None
            else:
                weather_types_list = weather_types.split(' ')
                weather_types_list = self.list_to_postgres_array(weather_types_list)
            drybulb_F = self.floatOrNA(row[header.index('DryBulbFarenheit')])
            wetbulb_F = self.floatOrNA(row[header.index('WetBulbFarenheit')])
            dewpoint_F = self.floatOrNA(row[header.index('DewPointFarenheit')])
            rel_humidity = self.integerOrNA(row[header.index('RelativeHumidity')])
            wind_speed = self.integerOrNA(row[header.index('WindSpeed')])
            # XX mcc consider handling WindSpeedFlag == 's' for 'suspect'
            wind_direction, wind_cardinal = self.getWind(wind_speed, row[header.index('WindDirection')])
            station_pressure = self.floatOrNA(row[header.index('StationPressure')])
            sealevel_pressure = self.floatOrNA(row[header.index('SeaLevelPressure')])
            hourly_precip = self.getPrecip(row[header.index('HourlyPrecip')])
            record_type = row[header.index('RecordType')]
            
            # There are two types of report types (column is called "RecordType" for some reason).
            # 1) AA - METAR (AVIATION ROUTINE WEATHER REPORT) - HOURLY
            # 2) SP - METAR SPECIAL REPORT
            # Special reports seem to occur at the same time (and have
            # largely the same content) as hourly reports, but under certain
            # adverse conditions (e.g. low visibility). 
            # As such, I believe it is sufficient to just use the 'AA' reports and keep
            # our composite primary key of (wban_code, datetime).
            if (record_type == 'SP'):
                continue

            writer.writerow([wban_code, weather_date, old_station_type, station_type, sky_condition, \
                             sky_condition_top, visibility, weather_types_list, \
                             drybulb_F, wetbulb_F, dewpoint_F,\
                             rel_humidity, wind_speed, wind_direction, wind_cardinal,\
                             station_pressure, sealevel_pressure, record_type, hourly_precip])
        return  self.clean_observations_hourly_info

    # list_to_postgres_array(list_string): convert to {blah, blah2, blah3} format for postgres.
    def list_to_postgres_array(self, l):
        return "{" +  ', '.join(l) + "}"

    def getWind(self, wind_speed, wind_direction):
        wind_cardinal = None
        if (wind_direction == 'VR ' or wind_direction =='M'):
            wind_direction='VRB'
            wind_cardinal = 'VRB'
        elif (wind_direction.strip() == ''):
            wind_direction =None
            wind_cardinal = None
        else:
            try:
                wind_direction_int = int(wind_direction)
            except ValueError, e:
                if (self.debug==True):
                    self.debug_outfile.write("ValueError: [%s], could not convert wind_direction '%s' to int\n" % (e, wind_direction))
                return None, None
            wind_cardinal = self.degToCardinal(int(wind_direction))
        if (wind_speed == 0):
            wind_direction = None
            wind_cardinal = None
        return wind_direction, wind_cardinal

    # from http://stackoverflow.com/questions/7490660/converting-wind-direction-in-angles-to-text-words
    def degToCardinal(self,num):
        val=int((num/22.5)+.5)
        arr=["N","NNE","NE","ENE","E","ESE", "SE", "SSE","S","SSW","SW","WSW","W","WNW","NW","NNW"]
        return arr[(val % 16)]
        
    def getPrecip(self, precip_str):
        precip_total = None
        precip_total = precip_str.strip()
        if (precip_total == 'T'):
            precip_total = .005 # 'Trace' precipitation = .005 inch or less
        precip_total = self.floatOrNA(precip_total)
        return precip_total
                        
    def floatOrNA(self, val):
        val_str = str(val)
        if (val_str == 'M'):
            return None
        if (val_str == 'err'):
            return None
        if (val_str == 'null'):
            return None
        if (val_str.strip() == ''):  # WindSpeed line
            return None
        else:
            try:
                fval = float(val)
            except ValueError, e:
                if (self.debug==True):
                    self.debug_outfile.write("ValueError: [%s], could not convert '%s' to float\n" % (e, val))
                return None
            return fval

    def integerOrNA(self, val):
        val_str = str(val)
        if (val == 'M'):
            return None
        if (val_str == 'err'):
            return None
        if (val_str == 'null'):
            return None
        if (val_str.strip() == ''):  # WindSpeed line
            return None
        else:
            try: 
                ival = int(val)
            except ValueError, e:
                if (self.debug==True):
                    self.debug_outfile.write("ValueError [%s] could not convert '%s' to int\n" % (e, val))
                return None
            return ival
    
    def _make_daily_table(self):
        self.daily_table = self._get_daily_table()
        self.daily_table.append_column(Column('id', BigInteger, primary_key=True))
        self.daily_table.create(engine, checkfirst=True)

    def _make_hourly_table(self):
        self.hourly_table = self._get_hourly_table()
        self.hourly_table.append_column(Column('id', BigInteger, primary_key=True))
        self.hourly_table.create(engine, checkfirst=True)

    def _get_daily_table(self, name='dat'):
        return Table('%s_weather_observations_daily' % name, Base.metadata,
                            Column('wban_code', String(5), nullable=False),
                            Column('date', Date, nullable=False),
                            Column('temp_max', Float),
                            Column('temp_min', Float),
                            Column('temp_avg', Float),
                            Column('departure_from_normal', Float),
                            Column('dewpoint_avg', Float),
                            Column('wetbulb_avg', Float),
                            #Column('weather_types', ARRAY(String(16))), # column 'CodeSum',
                            Column('weather_types', ARRAY(String)), # column 'CodeSum',
                            # XX: Not sure about meaning of 'Cool' and 'Heat' columns in daily table,
                            #     based on documentation.
                            Column('snowfall', Float),
                            Column('precip_total', Float),
                            Column('station_pressure', Float),
                            Column('sealevel_pressure', Float),
                            Column('resultant_windspeed', Float),
                            Column('resultant_winddirection', String(3)), # appears to be 00 (000) to 36 (360)
                            Column('resultant_winddirection_cardinal', String(3)), # e.g. NNE, NNW
                            Column('avg_windspeed', Float),
                            Column('max5_windspeed', Float),
                            Column('max5_winddirection', String(3)), # 000 through 360, M for missing
                            Column('max5_direction_cardinal', String(3)), # e.g. NNE, NNW
                            Column('max2_windspeed', Float), 
                            Column('max2_winddirection', String(3)), # 000 through 360, M for missing
                            Column('max2_direction_cardinal', String(3)), # e.g. NNE, NNW
                            UniqueConstraint('wban_code', 'date', name='%s_wban_date_ix' % name),
                            extend_existing=True) 

    def _get_hourly_table(self, name='dat'):
        return Table('%s_weather_observations_hourly' % name, Base.metadata,
                Column('wban_code', String(5), nullable=False),
                Column('datetime', DateTime, nullable=False),
                # AO1: without precipitation discriminator, AO2: with precipitation discriminator
                Column('old_station_type', String(3)),
                Column('station_type', Integer),
                Column('sky_condition', String),
                Column('sky_condition_top', String), # top-level sky condition, e.g.
                                                        # if 'FEW018 BKN029 OVC100'
                                                        # we have overcast at 10,000 feet (100 * 100).
                                                        # BKN017TCU means broken clouds at 1700 feet w/ towering cumulonimbus
                                                        # BKN017CB means broken clouds at 1700 feet w/ cumulonimbus
                Column('visibility', Float), #  in Statute Miles
                # XX in R: unique(unlist(strsplit(unlist(as.character(unique(x$WeatherType))), ' ')))
                #Column('weather_types', ARRAY(String(16))),
                Column('weather_types', ARRAY(String)),
                Column('drybulb_fahrenheit', Float), # These can be NULL bc of missing data
                Column('wetbulb_fahrenheit', Float), # These can be NULL bc of missing data
                Column('dewpoint_fahrenheit', Float),# These can be NULL bc of missing data
                Column('relative_humidity', Integer),
                Column('wind_speed', Integer),
                Column('wind_direction', String(3)), # 000 to 360
                Column('wind_direction_cardinal', String(3)), # e.g. NNE, NNW
                Column('station_pressure', Float),
                Column('sealevel_pressure', Float),
                Column('report_type', String), # Either 'AA' or 'SP'
                Column('hourly_precip', Float),
                UniqueConstraint('wban_code', 'datetime', name='%s_wban_datetime_ix' % name),
                extend_existing=True)

    def _extract_last_fname(self):
        # XX: not currently parsing tar files
        #tar_last = 
        #tar_last = datetime(2007, 5, 1, 0, 0)
        #tar_filename = '%s.tar.gz' % tar_last.strftime('%Y%m') 
        #print 'tar_filename'

        zip_last = datetime.now()
        zip_filename = 'QCLCD%s.zip' % zip_last.strftime('%Y%m') 
        return zip_filename

    def _extract_fname(self, year_num, month_num):
        curr_dt = datetime(year_num, month_num, 1, 0, 0)
        if ((year_num < 2007) or (year_num == 2007 and month_num < 5)):
            tar_filename =  '%s.tar.gz' % (curr_dt.strftime('%Y%m'))
            # do not parse tars for now
            return None
        else:
            zip_filename = 'QCLCD%s.zip' % curr_dt.strftime('%Y%m')
            return zip_filename

    def _extract_fnames(self):
        tar_start = datetime(1996, 7, 1, 0, 0)
        tar_end = datetime(2007, 5, 1, 0, 0)
        zip_start = datetime(2007, 5, 1, 0, 0)
        zip_end = datetime.now() + timedelta(days=30)
        tar_filenames = ['%s.tar.gz' % d.strftime('%Y%m') for d in \
            self._date_span(tar_start, tar_end)]
        zip_filenames = ['QCLCD%s.zip' % d.strftime('%Y%m') for d in \
            self._date_span(zip_start, zip_end)]
        # XX: ignoring tars for now
        return zip_filenames

    def _load_hourly(self, transformed_input):
        if (self.debug==True):
            transformed_input.seek(0) 
            f = open(os.path.join(self.data_dir, 'weather_etl_dump_hourly.txt'), 'w')
            f.write(transformed_input.getvalue())
            f.close()
        transformed_input.seek(0)
        self.src_hourly_table = self._get_hourly_table(name='src')
        self.src_hourly_table.drop(engine, checkfirst=True)
        self.src_hourly_table.create(engine)
        names = [c.name for c in self.hourly_table.columns if c.name != 'id']
        ins_st = "COPY src_weather_observations_hourly ("
        for idx, name in enumerate(names):
            if idx < len(names) - 1:
                ins_st += '%s, ' % name
            else:
                ins_st += '%s)' % name
        else:
            ins_st += "FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')"
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.copy_expert(ins_st, transformed_input)

        conn.commit()
        if (self.debug==True):
            print ("committed", sys.stdout)


    def _load_daily(self, transformed_input): 
        if (self.debug==True):
            transformed_input.seek(0) 
            f = open(os.path.join(self.data_dir, 'weather_etl_dump_daily.txt'), 'w')
            f.write(transformed_input.getvalue())
            f.close()
        transformed_input.seek(0)
        names = [c.name for c in self.daily_table.columns if c.name != 'id']
        self.src_daily_table = self._get_daily_table(name='src')
        self.src_daily_table.drop(engine, checkfirst=True)
        self.src_daily_table.create(engine)
        ins_st = "COPY src_weather_observations_daily ("
        for idx, name in enumerate(names):
            if idx < len(names) - 1:
                ins_st += '%s, ' % name
            else:
                ins_st += '%s)' % name
        else:
            ins_st += "FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')"
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.copy_expert(ins_st, transformed_input)

        conn.commit()
        if (self.debug == True):
            print ("committed", sys.stdout)

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
        self._make_station_table()
        self._load()

    def update(self):
        self._extract()
        self._transform()
        # Doing this just so self.station_table is defined
        self._make_station_table()
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

    def _make_station_table(self):
        self.station_table = Table('weather_stations', Base.metadata,
                Column('wban_code', String(5), primary_key=True),
                Column('station_name', String(50), nullable=False),
                Column('country', String(2), nullable=False),
                Column('state', String(2)),
                Column('call_sign', String(5)),
                Column('location', Geometry('POINT', srid=4326)),
                Column('elevation', Float),
                Column('begin', Date),
                Column('end', Date))
        self.station_table.create(engine, checkfirst=True)

    def _load(self):
        names = [c.name for c in self.station_table.columns]
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
            station = session.query(self.station_table).filter(self.station_table.c.wban_code == row['wban_code']).all()
            if not station:
                ins = self.station_table.insert().values(**row)
                conn.execute(ins)
