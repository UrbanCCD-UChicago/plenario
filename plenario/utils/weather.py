import requests
import os
import sys
import tarfile
import zipfile
import re
from ftplib import FTP
#from cStringIO import StringIO
from StringIO import StringIO
from csvkit.unicsv import UnicodeCSVReader, UnicodeCSVWriter, \
    UnicodeCSVDictReader, FieldSizeLimitError
from dateutil import parser
from datetime import datetime, date, timedelta
from dateutil import relativedelta
import operator

import calendar
from plenario.database import task_session as session, task_engine as engine, \
    Base
from plenario.settings import DATA_DIR
import sqlalchemy
from sqlalchemy import Table, Column, String, Date, DateTime, Integer, Float, \
    VARCHAR, BigInteger, and_, select, text, distinct, func
from sqlalchemy.dialects.postgresql import ARRAY
from geoalchemy2 import Geometry
from uuid import uuid4

from weather_metar import getMetar, getMetarVals, getAllCurrentWeather, getCurrentWeather

# from http://stackoverflow.com/questions/7490660/converting-wind-direction-in-angles-to-text-words
def degToCardinal(num):
    val=int((num/22.5)+.5)
    arr=["N","NNE","NE","ENE","E","ESE", "SE", "SSE","S","SSW","SW","WSW","W","WNW","NW","NNW"]
    return arr[(val % 16)]


class WeatherError(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)
        self.message = message

class WeatherETL(object):
    """ 
    Download, transform and insert weather data into plenario
    """

    # contents:
    # - initialize() functions (initialize(), initialize_month(), metar_initialize_current())
    # - _do_etl() (_do_etl(), _metar_do_etl())
    # - _cleanup_temp_tables, _metar_cleanup_temp_tables
    # - _add_location() (not called?)
    # - _update(), _update_metar():
    #      - Idea here is to create a new new_table which will represent the intersection (?) of src and dat -- only new records
    #      - We are eventually storing in dat_table
    #      - Raw incoming data is in src_table
    # - make_tables(), metar_make_tables()
    # - _extract(fname)
    #
    #
    
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

    current_row = None

    def __init__(self, data_dir=DATA_DIR, debug=False):
        self.base_url = 'http://www.ncdc.noaa.gov/orders/qclcd'
        self.data_dir = data_dir
        self.debug_outfile = sys.stdout
        self.debug = debug
        self.out_header = None
        if (self.debug == True):
            self.debug_filename = os.path.join(self.data_dir, 'weather_etl_debug_out.txt')
            sys.stderr.write( "writing out debug_file %s\n" % self.debug_filename)
            self.debug_outfile = open(self.debug_filename, 'w+')
        self.wban2callsign_map = self.build_wban2callsign_map()

    def build_wban2callsign_map(self):
        #stations_table = Table('weather_stations', Base.metadata, 
        #                       autoload=True, autoload_with=engine, extend_existing=True)
        # ask stations_table for all rows where wban_code and call_sign are defined
        sql = "SELECT wban_code, call_sign FROM weather_stations WHERE call_sign IS NOT NULL"
        conn = engine.contextual_connect()
        results = conn.execute(sql)
        wban_callsigns = results.fetchall()
        d = dict(wban_callsigns)
        return d

        
    # WeatherETL.initialize_last(): for debugging purposes, only initialize the most recent month of weather data.
    def initialize_last(self, start_line=0, end_line=None):
        self.make_tables()
        fname = self._extract_last_fname()
        raw_hourly, raw_daily, file_type = self._extract(fname)
        t_daily = self._transform_daily(raw_daily, file_type, start_line=start_line, end_line=end_line)
        self._load_daily(t_daily)
        t_hourly = self._transform_hourly(raw_hourly, file_type, start_line=start_line, end_line=end_line)
        self._load_hourly(t_hourly)
        self._update(span='daily')
        self._update(span='hourly')
        self._cleanup_temp_tables()

    def initialize(self):
        #print "WeatherETL.initialize()"
        self.make_tables()
        fnames = self._extract_fnames()
        for fname in fnames:
            if (self.debug==True):
                print "INITIALIZE: doing fname", fname
            self._do_etl(fname)

    def initialize_month(self, year, month, no_daily=False, no_hourly=False, weather_stations_list = None, banned_weather_stations_list = None, start_line=0, end_line=None):
        self.make_tables()
        fname = self._extract_fname(year,month)
        self._do_etl(fname, no_daily, no_hourly, weather_stations_list, banned_weather_stations_list, start_line, end_line)

    # Import current observations, whatever they may be, for the specified list of station WBANs and/or banned station WBANs.
    def metar_initialize_current(self, weather_stations_list = None, banned_weather_stations_list = None):
        self.metar_make_tables()
        # we want to pass this to some _metar_do_etl() function
        self._metar_do_etl(weather_stations_list, banned_weather_stations_list)
            


    ######################################################################
    # do_etl: perform the ETL on a given tar/zip file
    #    weather_stations_list: a list of WBAN codes (as strings) in order to only read a subset of station observations
    #    start_line, end_line: optional parameters for testing which will only read a subset of lines from the file
    ######################################################################
    def _do_etl(self, fname, no_daily=False, no_hourly=False, weather_stations_list=None, banned_weather_stations_list = None, start_line=0, end_line=None):

        raw_hourly, raw_daily, file_type = self._extract(fname)

        if (self.debug):
            self.debug_outfile.write("Extracting: %s\n" % fname)
        
        if (not no_daily):
            t_daily = self._transform_daily(raw_daily, file_type, 
                                            weather_stations_list=weather_stations_list,
                                            banned_weather_stations_list=banned_weather_stations_list,
                                            start_line=start_line, end_line=end_line)
        if (not no_hourly):
            t_hourly = self._transform_hourly(raw_hourly, file_type, 
                                              weather_stations_list=weather_stations_list, 
                                              banned_weather_stations_list=banned_weather_stations_list,
                                              start_line=start_line, end_line=end_line)
        if (not no_daily):
            self._load_daily(t_daily)                          # this actually imports the transformed StringIO csv
            self._update(span='daily')
            # self._add_location(span='daily') # XXX mcc: hmm
        if (not no_hourly):
            self._load_hourly(t_hourly)    # this actually imports the transformed StringIO csv
            self._update(span='hourly')
            # self._add_location(span='hourly') # XXX mcc: hmm
        #self._cleanup_temp_tables()

    def _metar_do_etl(self,  weather_stations_list = None, banned_weather_stations_list = None):
        # Below code hits the METAR server
        # Don't bother calling any _extract_metar() function...

        #metar_codes = getAllCurrentWeather()
        if weather_stations_list:
            # map wbans to call signs.
            metar_codes = getCurrentWeather(wban_codes =weather_stations_list, wban2callsigns = self.wban2callsign_map)
        else:
            metar_codes = getAllCurrentWeather()
            
        
        t_metars = self._transform_metars(metar_codes,
                                          weather_stations_list,
                                          banned_weather_stations_list)

        #print "t_metars are: " ,t_metars
        self._load_metar(t_metars)
        self._update_metar()
        self._metar_cleanup_temp_tables()

    def _cleanup_temp_tables(self):
        for span in ['daily', 'hourly']:
            for tname in ['src', 'new']:
                try:
                    table = getattr(self, '%s_%s_table' % (tname, span)) # possibly not getting dropped
                    table.drop(engine, checkfirst=True)
                except AttributeError:
                    continue

    def _metar_cleanup_temp_tables(self):
        for tname in ['src', 'new']:
            try:
                table = getattr(self, '%s_metar_table' % tname)
                table.drop(engine, checkfirst=True)
            except AttributeError:
                continue

    def _add_location(self, span=None):
        """ 
        Add latitude and longitude from weather station into observations table
        """
        start_day, end_day = calendar.monthrange(self.current_year, self.current_month)
        range_start = '%s-%s-%s' % (self.current_year, self.current_month, 1)
        range_end = '%s-%s-%s' % (self.current_year, self.current_month, end_day)
        date_col = 'date'
        table_name = 'dat_weather_observations_%s' % span
        if span == 'hourly' or span == 'metar':
            date_col = 'datetime'
        upd = text("""
            UPDATE %s SET 
                longitude = subq.longitude,
                latitude = subq.latitude
            FROM (
                SELECT 
                    wban_code,
                    st_x(location) as longitude,
                    st_y(location) as latitude
                FROM weather_stations
            ) as subq
            WHERE %s.wban_code = subq.wban_code
               AND %s.%s <= :range_end
               AND %s.%s >= :range_start
               AND %s.longitude IS NULL
               AND %s.latitude IS NULL
        """ % (table_name, table_name, 
               table_name, date_col,
               table_name, date_col,
               table_name, table_name)
        )
        conn = engine.contextual_connect()
        conn.execute(upd, range_start=range_start, range_end=range_end)


        
    def _update(self, span=None):
        new_table = Table('new_weather_observations_%s' % span, Base.metadata,
                          Column('wban_code', String(5)), keep_existing=True)
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
        #print "_update: span=%s: sql is'%s'" % (span, ins)
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
            #print "_update NEW : span=%s: sql is'%s'" % (span, ins)
            conn.execute(ins)

    def _update_metar(self):
        #print "_update_metar()"
        
        new_table = Table('new_weather_observations_metar', Base.metadata,
                          Column('wban_code', String(5)), keep_existing=True)  # intersection of src and dat -- only new records
        dat_table = getattr(self, 'metar_table') # where we are eventually storing things
        src_table = getattr(self, 'src_metar_table') # raw incoming data

        #print "we have new_table: '%s'" % new_table
        #print "we have dat_table: '%s'" % dat_table
        #print "we have src_table: '%s'" % src_table
        
        from_sel_cols = ['wban_code', 'datetime']
        src_date_col = src_table.c.datetime
        dat_date_col = dat_table.c.datetime
        new_table.append_column(Column('datetime', DateTime))
        new_date_col = new_table.c.datetime
        new_table.drop(engine, checkfirst=True)
        try:
            new_table.create(engine)
        except sql.exc.ProgrammingError:
            print "got ProgrammingError on new metar table create"
            return None

        ## Essentially, insert into the new observations table for any src observations that are not in the current dat observations.
        #ins = """
        #INSERT INTO new_weather_observations_metar (wban_code, date)
        #SELECT src_weather_observations_metar.wban_code, src_weather_observations_metar.datetime
        #FROM src_weather_observations_metar 
        #LEFT OUTER JOIN dat_weather_observations_metar 
        #ON src_weather_observations_metar.wban_code = dat_weather_observations_metar.wban_code 
        #AND src_weather_observations_metar.datetime = dat_weather_observations_metar.datetime
        #WHERE dat_weather_observations_metar.id IS NULL'
        #"""

        ins = new_table.insert()\
                .from_select(from_sel_cols, 
                    select([src_table.c.wban_code, src_date_col])\
                        .select_from(src_table.join(dat_table,
                            and_(src_table.c.wban_code == dat_table.c.wban_code,
                                 src_date_col == dat_date_col),
                            isouter=True)
                    ).where(dat_table.c.id == None)
                )


        #print "_update_metar(): sql is'%s'" % ins
        conn = engine.contextual_connect()
        
        try:
            conn.execute(ins)
            new = True
        except TypeError:
            new = False
        except sql.exc.ProgrammingError:
            print "got ProgrammingError on insert to new table"
        if new:

            # There were no new records.. soo, insert into the dat observations any records
            # from src observations which match records in new observations.

            # 'INSERT INTO dat_weather_observations_daily (wban_code, date, temp_max, temp_min, temp_avg, departure_from_normal, dewpoint_avg, wetbulb_avg, weather_types, snowice_depth, snowice_waterequiv, snowfall, precip_total, station_pressure, sealevel_pressure, resultant_windspeed, resultant_winddirection, resultant_winddirection_cardinal, avg_windspeed, max5_windspeed, max5_winddirection, max5_direction_cardinal, max2_windspeed, max2_winddirection, max2_direction_cardinal, longitude, latitude)

            #ins = """
            #INSERT INTO dat_weather_observations_metar (wban_code, call_sign, datetime, sky_condition, sky_condition_top, visibility
            #SELECT ...
            #"""
            
            ins = dat_table.insert()\
                    .from_select([c for c in dat_table.columns if c.name != 'id'], 
                        select([c for c in src_table.columns])\
                            .select_from(src_table.join(new_table,
                                and_(src_table.c.wban_code == new_table.c.wban_code,
                                     src_date_col == new_date_col)
                            ))
                    )
            conn.execute(ins)

            
    def make_tables(self):
        self._make_daily_table()
        self._make_hourly_table()

    def metar_make_tables(self):
        self._make_metar_table()
            
    ########################################
    ########################################
    # Extract (from filename / URL to some raw StringIO()
    ########################################
    ########################################
    def _download_write(self, fname):
        fpath = os.path.join(self.data_dir, fname)
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

    def _extract(self, fname):
        file_type = 'zipfile'

        if fname.endswith('.zip'):
            file_type = 'zipfile'
        elif fname.endswith('tar.gz'):
            file_type = 'tarfile'
        else:
            print "file type for ", fname, "not found: quitting"
            return None

        # extract the year and month from the QCLCD filename
        fname_spl = fname.split('.')
        # look at the 2nd to last string
        fname_yearmonth = (fname_spl[:-1])[0]
        yearmonth_str = fname_yearmonth[-6:]
        year_str = yearmonth_str[0:4]
        month_str = yearmonth_str[4:6]
        
        fpath = os.path.join(self.data_dir, fname)
        raw_weather_hourly = StringIO()
        raw_weather_daily = StringIO()
        
        now_month, now_year = str(datetime.now().month), str(datetime.now().year)
        if '%s%s' % (now_year.zfill(2), now_month.zfill(2)) == yearmonth_str:
            self._download_write(fname)

        elif not os.path.exists(fpath):
            self._download_write(fname)

        if file_type == 'tarfile':
            with tarfile.open(fpath, 'r') as tar:
                for tarinfo in tar:
                    if tarinfo.name.endswith('hourly.txt') and (yearmonth_str in tarinfo.name):
                        raw_weather_hourly.write(tar.extractfile(tarinfo).read())
                    elif tarinfo.name.endswith('daily.txt') and (yearmonth_str in tarinfo.name): 
                        # need this 2nd caveat above to handle ridiculous stuff like 200408.tar.gz containing 200512daily.txt for no reason
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

    ########################################
    ########################################
    # Transformations of daily data e.g. '200704daily.txt' (from tarfile) or '201101daily.txt' (from zipfile)
    ########################################
    ########################################
    def _transform_daily(self, raw_weather, file_type,  weather_stations_list = None, banned_weather_stations_list=None, start_line=0, end_line=None):
        raw_weather.seek(0)
        # NOTE: not using UnicodeCSVReader because it.. (ironically) won't let us bail on a unicode error [mcc]
        #reader = UnicodeCSVReader(raw_weather)
        #header = reader.next() # skip header
        raw_header= raw_weather.readline() # skip header
        raw_header.strip()
        header = raw_header.split(',')
        header = [x.strip() for x in header]
        #print "header is ", header

        self.clean_observations_daily = StringIO()
        writer = UnicodeCSVWriter(self.clean_observations_daily)
        self.out_header = ["wban_code","date","temp_max","temp_min",
                           "temp_avg","departure_from_normal",
                           "dewpoint_avg", "wetbulb_avg","weather_types",
                           "snowice_depth", "snowice_waterequiv",
                           "snowfall","precip_total", "station_pressure",
                           "sealevel_pressure", 
                           "resultant_windspeed", "resultant_winddirection", "resultant_winddirection_cardinal",
                           "avg_windspeed",
                           "max5_windspeed", "max5_winddirection","max5_winddirection_cardinal",
                           "max2_windspeed", "max2_winddirection","max2_winddirection_cardinal"]
        writer.writerow(self.out_header)

        row_count = 0
        while True:
            try:
                #row = reader.next()
                # DIY csv parsing for QCLCD to avoid buffering issues in UnicodeCVSReader
                raw_row = raw_weather.readline()
                if (raw_row == ''):
                    break
                raw_row.strip()
                row = raw_row.split(',')
                
                self.current_row = row
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
             
                if (len(row) == 0):
                    continue

                row_vals = getattr(self, '_parse_%s_row_daily' % file_type)(row, header, self.out_header)
             
                row_dict = dict(zip(self.out_header, row_vals))
                if (weather_stations_list is not None):
                    # Only include observations from the specified list of wban_code values
                    if (row_dict['wban_code'] not in weather_stations_list):
                        continue
             
                writer.writerow(row_vals)
            except UnicodeDecodeError:
                if (self.debug == True):
                    self.debug_outfile.write("UnicodeDecodeError caught\n")
                    self.debug_outfile.write(str(row))
                    self.debug_outfile.write(str(row_count) + "\n" + str(zip(self.out_header,row)) + "\n")
                    self.debug_outfile.flush()
                # Skip this line, it has non-ASCII characters and probably shouldn't.
                # We may not get this error anymore (ironically) after rolling our own CSV parser
                #   as opposed to UnicodeCSVReeder
                pass
                
                continue
            except StopIteration:
                break

        self.debug_outfile.write('finished %s rows\n' % row_count)
        return self.clean_observations_daily


    def _parse_zipfile_row_daily(self, row, header, out_header):
        wban_code = row[header.index('WBAN')]
        date = row[header.index('YearMonthDay')] # e.g. 20140801
        temp_max = self.getTemp(row[header.index('Tmax')])
        temp_min = self.getTemp(row[header.index('Tmin')])
        temp_avg = self.getTemp(row[header.index('Tavg')])
        departure_from_normal = self.floatOrNA(row[header.index('Depart')])
        dewpoint_avg = self.floatOrNA(row[header.index('DewPoint')])
        wetbulb_avg = self.floatOrNA(row[header.index('WetBulb')])
        weather_types_list = self._parse_weather_types(row[header.index('CodeSum')])
        snowice_depth = self.getPrecip(row[header.index('Depth')])
        snowice_waterequiv = self.getPrecip(row[header.index('Water1')]) # predict 'heart-attack snow'!
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

        vals = [wban_code,date,temp_max,temp_min,
                temp_avg,departure_from_normal,
                dewpoint_avg, wetbulb_avg,weather_types_list,
                snowice_depth, snowice_waterequiv,
                snowfall,precip_total, station_pressure,
                sealevel_pressure, 
                resultant_windspeed, resultant_winddirection, resultant_winddirection_cardinal,
                avg_windspeed,
                max5_windspeed, max5_winddirection,max5_winddirection_cardinal,
                max2_windspeed, max2_winddirection, max2_winddirection_cardinal]

        assert(len(out_header) == len(vals))
        
        return vals

    def _parse_tarfile_row_daily(self, row, header, out_header):
        wban_code = self.getWBAN(row[header.index('Wban Number')])
        date = row[header.index('YearMonthDay')] # e.g. 20140801
        temp_max = self.getTemp(row[header.index('Max Temp')])
        temp_min = self.getTemp(row[header.index('Min Temp')])
        temp_avg = self.getTemp(row[header.index('Avg Temp')])
        departure_from_normal = self.floatOrNA(row[header.index('Dep from Normal')])
        dewpoint_avg = self.floatOrNA(row[header.index('Avg Dew Pt')])
        wetbulb_avg = self.floatOrNA(row[header.index('Avg Wet Bulb')])
        weather_types_list = self._parse_weather_types(row[header.index('Significant Weather')])
        snowice_depth = self.getPrecip(row[header.index('Snow/Ice Depth')])
        snowice_waterequiv = self.getPrecip(row[header.index('Snow/Ice Water Equiv')]) # predict 'heart-attack snow'!
        snowfall = self.getPrecip(row[header.index('Precipitation Snowfall')])
        precip_total= self.getPrecip(row[header.index('Precipitation Water Equiv')])
        station_pressure=self.floatOrNA(row[header.index('Pressue Avg Station')]) # XXX Not me -- typo in header!
        sealevel_pressure=self.floatOrNA(row[header.index('Pressure Avg Sea Level')])
        resultant_windspeed = self.floatOrNA(row[header.index('Wind Speed')])
        resultant_winddirection, resultant_winddirection_cardinal=self.getWind(resultant_windspeed, row[header.index('Wind Direction')])
        avg_windspeed=self.floatOrNA(row[header.index('Wind Avg Speed')])            
        max5_windspeed=self.floatOrNA(row[header.index('Max 5 sec speed')])
        max5_winddirection, max5_winddirection_cardinal=self.getWind(max5_windspeed, row[header.index('Max 5 sec Dir')])
        max2_windspeed=self.floatOrNA(row[header.index('Max 2 min speed')])
        max2_winddirection, max2_winddirection_cardinal=self.getWind(max2_windspeed, row[header.index('Max 2 min Dir')])

        vals= [wban_code,date,temp_max,temp_min,
               temp_avg,departure_from_normal,
               dewpoint_avg, wetbulb_avg,weather_types_list,
               snowice_depth, snowice_waterequiv,
               snowfall,precip_total, station_pressure,
               sealevel_pressure, 
               resultant_windspeed, resultant_winddirection, resultant_winddirection_cardinal,
               avg_windspeed,
               max5_windspeed, max5_winddirection,max5_winddirection_cardinal,
               max2_windspeed, max2_winddirection, max2_winddirection_cardinal]

        assert(len(out_header) == len(vals))
        
        return vals


    ########################################
    ########################################
    # Transformations of hourly data e.g. 200704hourly.txt (from tarfile) or 201101hourly.txt (from zipfile)
    ########################################
    ########################################
    def _transform_hourly(self, raw_weather, file_type,  weather_stations_list = None,  banned_weather_stations_list=None, start_line=0, end_line=None):
        raw_weather.seek(0)
        # XXX mcc: should probably convert this to DIY CSV parsing a la _transform_daily()
        reader = UnicodeCSVReader(raw_weather)
        header = reader.next()
        # strip leading and trailing whitespace from header (e.g. from tarfiles)
        header = [x.strip() for x in header]

        self.clean_observations_hourly = StringIO()
        writer = UnicodeCSVWriter(self.clean_observations_hourly)
        self.out_header = ["wban_code","datetime","old_station_type","station_type", \
                           "sky_condition","sky_condition_top","visibility",\
                           "weather_types","drybulb_fahrenheit","wetbulb_fahrenheit",\
                           "dewpoint_fahrenheit","relative_humidity",\
                           "wind_speed","wind_direction","wind_direction_cardinal",\
                           "station_pressure","sealevel_pressure","report_type",\
                           "hourly_precip"]
        writer.writerow(self.out_header)

        row_count = 0
        while True:
            try:
                row = reader.next()
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
             
                if (len(row) == 0):
                    continue
             
                # this calls either self._parse_zipfile_row_hourly
                # or self._parse_tarfile_row_hourly
                row_vals = getattr(self, '_parse_%s_row_hourly' % file_type)(row, header, self.out_header)
                if (not row_vals):
                    continue
             
                row_dict = dict(zip(self.out_header, row_vals))
                if (weather_stations_list is not None):
                    # Only include observations from the specified list of wban_code values
                    if (row_dict['wban_code'] not in weather_stations_list):
                        continue
                    
                if (banned_weather_stations_list is not None):
                    if (row_dict['wban_code'] in banned_weather_stations_list):
                        continue
                
                
             
                writer.writerow(row_vals)
            except FieldSizeLimitError:
                continue
            except StopIteration:
                break
        return self.clean_observations_hourly

    def _parse_zipfile_row_hourly(self, row, header, out_header):
        # There are two types of report types (column is called "RecordType" for some reason).
        # 1) AA - METAR (AVIATION ROUTINE WEATHER REPORT) - HOURLY
        # 2) SP - METAR SPECIAL REPORT
        # Special reports seem to occur at the same time (and have
        # largely the same content) as hourly reports, but under certain
        # adverse conditions (e.g. low visibility). 
        # As such, I believe it is sufficient to just use the 'AA' reports and keep
        # our composite primary key of (wban_code, datetime).
        report_type = row[header.index('RecordType')]

        wban_code = row[header.index('WBAN')]
        date = row[header.index('Date')] # e.g. 20140801
        time = row[header.index('Time')] # e.g. '601' 6:01am
        # pad this into a four digit number:
        time_str = None
        if (time):
            time_int =  self.integerOrNA(time)
            time_str = '%04d' % time_int
        
        weather_date = datetime.strptime('%s %s' % (date, time_str), '%Y%m%d %H%M')
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
        weather_types_list = self._parse_weather_types(row[header.index('WeatherType')])
        weather_types_flag = row[header.index('WeatherTypeFlag')]
        # XX mcc consider handling weather_type_flag =='s' for 'suspect'
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
            
        vals= [wban_code,
               weather_date, 
               old_station_type,
               station_type,
               sky_condition, sky_condition_top,
               visibility, 
               weather_types_list,
               drybulb_F,
               wetbulb_F,
               dewpoint_F,
               rel_humidity,
               wind_speed, wind_direction, wind_cardinal,
               station_pressure, sealevel_pressure,
               report_type,
               hourly_precip]

        assert(len(out_header) == len(vals))

        # return hourly zipfile params
        return vals

    def _parse_tarfile_row_hourly(self, row, header, out_header):
        report_type = row[header.index('Record Type')]
        if (report_type == 'SP'):
            return None

        wban_code = row[header.index('Wban Number')]
        wban_code = wban_code.lstrip('0') # remove leading zeros from WBAN
        date = row[header.index('YearMonthDay')] # e.g. 20140801
        time = row[header.index('Time')] # e.g. '601' 6:01am
        # pad this into a four digit number:
        time_str = None
        if (time): 
            time_int = self.integerOrNA(time)
            if not time_int:
                time_str = None
                # XX: maybe just continue and bail if this doesn't work
                return None
            time_str = '%04d' % time_int
        try:
            weather_date = datetime.strptime('%s %s' % (date, time_str), '%Y%m%d %H%M')
        except ValueError:
            # This means the date / time can't be parsed and is probably not reliable.
            return None
        old_station_type = row[header.index('Station Type')].strip() # either AO1, AO2, or '-' (XX: why '-'??)
        station_type = None
        sky_condition = row[header.index('Sky Conditions')].strip()
        sky_condition_top = sky_condition.split(' ')[-1]
        
        visibility = self._parse_old_visibility(row[header.index('Visibility')])

        weather_types_list = self._parse_weather_types(row[header.index('Weather Type')])
        
        drybulb_F = self.floatOrNA(row[header.index('Dry Bulb Temp')])
        wetbulb_F = self.floatOrNA(row[header.index('Wet Bulb Temp')])
        dewpoint_F = self.floatOrNA(row[header.index('Dew Point Temp')])
        rel_humidity = self.integerOrNA(row[header.index('% Relative Humidity')])
        wind_speed = self.integerOrNA(row[header.index('Wind Speed (kt)')])
        wind_direction, wind_cardinal = self.getWind(wind_speed, row[header.index('Wind Direction')])
        station_pressure = self.floatOrNA(row[header.index('Station Pressure')])
        sealevel_pressure = self.floatOrNA(row[header.index('Sea Level Pressure')])
        hourly_precip = self.getPrecip(row[header.index('Precip. Total')])
        
        vals= [wban_code,
               weather_date, 
               old_station_type,station_type,
               sky_condition, sky_condition_top,
               visibility, 
               weather_types_list,
               drybulb_F,
               wetbulb_F,
               dewpoint_F,
               rel_humidity,
               wind_speed, wind_direction, wind_cardinal,
               station_pressure, sealevel_pressure,
               report_type,
               hourly_precip]

        assert(len(out_header) == len(vals))

        return vals

    def _transform_metars(self, metar_codes, weather_stations_list=None, banned_weather_stations_list=None):
        metar_codes_idx = 0

        self.clean_observations_metar = StringIO()
        writer = UnicodeCSVWriter(self.clean_observations_metar)
        self.out_header=["wban_code", "call_sign", "datetime", "sky_condition", "sky_condition_top",
                         "visibility", "weather_types", "temp_fahrenheit", "dewpoint_fahrenheit",
                         "wind_speed", "wind_direction", "wind_direction_cardinal", "wind_gust",
                         "station_pressure", "sealevel_pressure",
                         "precip_1hr","precip_3hr", "precip_6hr", "precip_24hr"]

        writer.writerow(self.out_header)
        row_count = 0
        for row in metar_codes:
            row_count += 1
            row_vals = self._parse_row_metar(row, self.out_header)

            # XXX: convert row_dict['weather_types'] from a list of lists (e.g. [[None, None, None, '', 'BR', None]])
            # to a string that looks like: "{{None, None, None, '', 'BR', None}}"

            weather_types_str = str(row_vals[self.out_header.index('weather_types')])
            #print "weather_types_str = '%s'" % weather_types_str
            # literally just change '[' to '{' and ']' to '}'
            weather_types_str = weather_types_str.replace('[', '{')
            weather_types_str = weather_types_str.replace(']', '}')
            row_vals[self.out_header.index('weather_types')] = weather_types_str
            #print "_transform_metars(): changed row_vals[self.out_header.index('weather_types')] to ", weather_types_str
            
            if (not row_vals):
                continue
            row_dict = dict(zip(self.out_header, row_vals))

            if (weather_stations_list is not None):
                # Only include observations from the specified list of wban_code values
                if (row_dict['wban_code'] not in weather_stations_list):
                    continue
                    
            if (banned_weather_stations_list is not None):
                if (row_dict['wban_code'] in banned_weather_stations_list):
                    continue
            if (self.debug==True):
                print "_transform_metars(): WRITING row_vals: '%s'"  % str(row_vals)
            writer.writerow(row_vals)

        return self.clean_observations_metar



    def _parse_row_metar(self, row, header):
        import metar

        try:
            m = getMetar(row)
        except metar.ParserError:
            return []
        
        vals = getMetarVals(m)
        #print "_parse_row_metar(): header=", header
        #print "_parse_row_metar(): vals=",vals
        assert(len(header) == len(vals))        
        return vals


    
    
    
    # Help parse a 'present weather' string like 'FZFG' (freezing fog) or 'BLSN' (blowing snow) or '-RA' (light rain)
    # When we are doing precip slurp as many as possible
    def _do_weather_parse(self, pw, mapping, multiple=False, local_debug=False):

        # Grab as many of the keys as possible consecutively in the string
        retvals = []
        while (multiple == True):
            (pw, key) = self._do_weather_parse(pw, mapping, multiple=False, local_debug=True)
            #print "got pw, key=", pw,key
            retvals.append(key)
            if ((pw == '') or (key == 'NULL')):
                return pw, retvals
                break
            else:
                continue

        if (len(pw) == 0): 
            return ('', 'NULL')

        # 2nd parse for descriptors
        for (key, val) in mapping:
            #print "key is '%s'" % key
            q = pw[0:len(key)]
            if (q == key):
                #print "key found: ", q
                pw2=pw[len(key):]
                #print "returning", l2
                #return (l2, val)
                return (pw2, key)
        return (pw, 'NULL')

    # Parse a 'present weather' string like 'FZFG' (freezing fog) or 'BLSN' (blowing snow) or '-RA' (light rain)
    def _parse_present_weather(self, pw):
        orig_pw = pw
        l = pw

        intensities =  [('-','Light'),
                        ('+','Heavy')]

        (l, intensity) = self._do_weather_parse(l, intensities)

        vicinities = [('VC','Vicinity')]
        (l, vicinity) = self._do_weather_parse(l, vicinities)
        
        descriptors = [('MI','Shallow'),
                       ('PR','Partial'),
                       ('BC','Patches'),
                       ('DR','Low Drifting'),
                       ('BL','Blowing'),
                       ('SH','Shower(s)'),
                       ('TS','Thunderstorm'),
                       ('FZ','Freezing')]
            
        (l, desc)= self._do_weather_parse(l, descriptors)
        
        # 3rd parse for phenomena
        precip_phenoms= [('DZ','Drizzle'),
                         ('RA','Rain'),
                         ('SN','Snow'),
                         ('SG','Snow Grains'),
                         ('IC','Ice Crystals'),
                         ('PE','Ice Pellets'),
                         ('PL','Ice Pellets'),
                         ('GR','Hail'),
                         ('GS','Small Hail'),
                         ('UP','Unknown Precipitation')]
        # We use arrays instead of hashmaps because we want to look for FG+ before FG (sigh)
        obscuration_phenoms  = [('BR','Mist'),
                                ('FG+','Heavy Fog'),
                                ('FG','Fog'),
                                ('FU','Smoke'),
                                ('VA','Volcanic Ash'),
                                ('DU','Widespread Dust'),
                                ('SA','Sand'),
                                ('HZ','Haze'),
                                ('PY','Spray')]
        other_phenoms = [('PO','Dust Devils'),
                         ('SQ','Squalls'),
                         ('FC','Funnel Cloud'),
                         ('+FC','Tornado Waterspout'),
                         ('SS','Sandstorm'),
                         ('DS','Duststorm'),
                         ('GL','Glaze')]
                
        (l, precips) = self._do_weather_parse(l, precip_phenoms, multiple =True)
        (l, obscuration) = self._do_weather_parse(l, obscuration_phenoms)
        (l, other) = self._do_weather_parse(l, other_phenoms)

        # if l still has a length let's print it out and see what went wrong
        if (self.debug==True):
            if (len(l) > 0):
                self.debug_outfile.write("\n")
                self.debug_outfile.write(str(self.current_row))
                self.debug_outfile.write("\ncould not fully parse present weather : '%s' '%s'\n\n" % ( orig_pw, l))
        wt_list = [intensity, vicinity, desc, precips[0], obscuration, other]
    
        ret_wt_lists = []
        ret_wt_lists.append(wt_list)
        
        #if (len(precips) > 1):
        #    print "first precip: ", wt_list
        for p in precips[1:]:
            if p != 'NULL':
                #print "extra precip!", p, orig_pw
                wt_list = ['NULL', 'NULL', 'NULL', p, 'NULL', 'NULL']
                #print "extra precip (precip):", wt_list
                ret_wt_lists.append(wt_list)
        
        return ret_wt_lists
        


    # Parse a list of 'present weather' strings and convert to multidimensional postgres array.
    def _parse_weather_types(self, wt_str):
        wt_str = wt_str.strip()
        if ((wt_str == '') or (wt_str == '-')):
            return None
        if (not wt_str):
            return None
        else:
            wt_list = wt_str.split(' ')
            wt_list = [wt.strip() for wt in wt_list]
            pw_lists = []

            for wt in wt_list:
                wts = self._parse_present_weather(wt)
                # make all weather reports have the same length..
                for obsv in wts:
                    wt_list3 = self.list_to_postgres_array(obsv)
                    pw_lists.append(wt_list3)
            list_of_lists = "{" +  ', '.join(pw_lists) + "}"
            #print "list_of_lists: "  , list_of_lists
            return list_of_lists

    def _parse_old_visibility(self, visibility_str):
        visibility_str = visibility_str.strip()
        
        visibility_str = re.sub('SM', '', visibility_str)
        # has_slash = re.match('\/'), visibility_str)
        # XX This is not worth it, too many weird, undocumented special cases on this particular column
        return None


    # list_to_postgres_array(list_string): convert to {blah, blah2, blah3} format for postgres.
    def list_to_postgres_array(self, l):
        return "{" +  ', '.join(l) + "}"

    def getWBAN(self,wban):
        return wban
    
    def getTemp(self, temp):
        if temp[-1] == '*':
            temp = temp[:-1]
        return self.floatOrNA(temp)
        
    def getWind(self, wind_speed, wind_direction):
        wind_cardinal = None
        wind_direction = wind_direction.strip()
        if (wind_direction == 'VR' or wind_direction =='M' or wind_direction=='VRB'):
            wind_direction='VRB'
            wind_cardinal = 'VRB'
        elif (wind_direction == '' or wind_direction == '-'):
            wind_direction =None
            wind_cardinal = None
        else:
            wind_direction_int = None
            try:
                # XXX: NOTE: rounding wind_direction to integer. Sorry.
                # XXX: Examine this field more carefully to determine what its range is.
                wind_direction_int = int(round(float(wind_direction)))
                wind_direction = wind_direction_int
            except ValueError, e:
                if (self.debug==True):
                    if (self.current_row): 
                        self.debug_outfile.write("\n")                        
                        zipped_row = zip(self.out_header,self.current_row)
                        for column in zipped_row:
                            self.debug_outfile.write(str(column) + "\n")
                    self.debug_outfile.write("\nValueError: [%s], could not convert wind_direction '%s' to int\n" % (e, wind_direction))
                    self.debug_outfile.flush()
                return None, None

            wind_cardinal = degToCardinal(wind_direction_int)
        if (wind_speed == 0):
            wind_direction = None
            wind_cardinal = None
        return wind_direction, wind_cardinal

        
    def getPrecip(self, precip_str):
        precip_total = None
        precip_total = precip_str.strip()
        if (precip_total == 'T'):
            precip_total = .005 # 'Trace' precipitation = .005 inch or less
        precip_total = self.floatOrNA(precip_total)
        return precip_total
                        
    def floatOrNA(self, val):
        val_str = str(val).strip()
        if (val_str == 'M'):
            return None
        if (val_str == '-'):
            return None
        if (val_str == 'err'):
            return None
        if (val_str == 'null'):
            return None
        if (val_str == ''):  # WindSpeed line
            return None
        else:
            try:
                fval = float(val_str)
            except ValueError, e:
                if (self.debug==True):
                    if (self.current_row): 
                        self.debug_outfile.write("\n")
                        zipped_row = zip(self.out_header,self.current_row)
                        for column in zipped_row:
                            self.debug_outfile.write(str(column)+ "\n")
                    self.debug_outfile.write("\nValueError: [%s], could not convert '%s' to float\n" % (e, val_str))
                    self.debug_outfile.flush()
                return None
            return fval

    def integerOrNA(self, val):
        val_str = str(val).strip()
        if (val_str == 'M'):
            return None
        if (val_str == '-'):
            return None
        if (val_str == 'VRB'):
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
                    if (self.current_row): 
                        self.debug_outfile.write("\n")
                        zipped_row = zip(self.out_header,self.current_row)
                        for column in zipped_row:
                            self.debug_outfile.write(str(column) + "\n")
                    self.debug_outfile.write("\nValueError [%s] could not convert '%s' to int\n" % (e, val))
                    self.debug_outfile.flush()
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

    def _make_metar_table(self):
        self.metar_table = self._get_metar_table()
        self.metar_table.append_column(Column('id', BigInteger, primary_key=True))
        self.metar_table.create(engine, checkfirst=True)
        
    def _get_daily_table(self, name='dat'):
        return Table('%s_weather_observations_daily' % name, Base.metadata,
                            Column('wban_code', String(5), nullable=False),
                            Column('date', Date, nullable=False),
                            Column('temp_max', Float, index=True),
                            Column('temp_min', Float, index=True),
                            Column('temp_avg', Float, index=True),
                            Column('departure_from_normal', Float),
                            Column('dewpoint_avg', Float),
                            Column('wetbulb_avg', Float),
                            #Column('weather_types', ARRAY(String(16))), # column 'CodeSum',
                            Column('weather_types', ARRAY(String)), # column 'CodeSum',
                            Column("snowice_depth", Float),
                            Column("snowice_waterequiv", Float),
                            # XX: Not sure about meaning of 'Cool' and 'Heat' columns in daily table,
                            #     based on documentation.
                            Column('snowfall', Float),
                            Column('precip_total', Float, index=True),
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
                            Column('longitude', Float),
                            Column('latitude', Float),
                            keep_existing=True) 

    def _get_hourly_table(self, name='dat'):
        return Table('%s_weather_observations_hourly' % name, Base.metadata,
                Column('wban_code', String(5), nullable=False),
                Column('datetime', DateTime, nullable=False),
                # AO1: without precipitation discriminator, AO2: with precipitation discriminator
                Column('old_station_type', String(5)),
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
                Column('drybulb_fahrenheit', Float, index=True), # These can be NULL bc of missing data
                Column('wetbulb_fahrenheit', Float), # These can be NULL bc of missing data
                Column('dewpoint_fahrenheit', Float),# These can be NULL bc of missing data
                Column('relative_humidity', Integer),
                Column('wind_speed', Integer),
                Column('wind_direction', String(3)), # 000 to 360
                Column('wind_direction_cardinal', String(3)), # e.g. NNE, NNW
                Column('station_pressure', Float),
                Column('sealevel_pressure', Float),
                Column('report_type', String), # Either 'AA' or 'SP'
                Column('hourly_precip', Float, index=True),
                Column('longitude', Float),
                Column('latitude', Float),
                keep_existing=True)

    def _get_metar_table(self, name='dat'):
        return Table('%s_weather_observations_metar' % name, Base.metadata,
                Column('wban_code', String(5), nullable=False),
                Column('call_sign', String(5), nullable=False),  
                Column('datetime', DateTime, nullable=False),
                Column('sky_condition', String),
                Column('sky_condition_top', String), # top-level sky condition, e.g.
                                                        # if 'FEW018 BKN029 OVC100'
                                                        # we have overcast at 10,000 feet (100 * 100).
                                                        # BKN017TCU means broken clouds at 1700 feet w/ towering cumulonimbus
                                                        # BKN017CB means broken clouds at 1700 feet w/ cumulonimbus
                Column('visibility', Float), #  in Statute Miles
                Column('weather_types', ARRAY(String)),
                Column('temp_fahrenheit', Float, index=True), # These can be NULL bc of missing data
                Column('dewpoint_fahrenheit', Float),# These can be NULL bc of missing data
                Column('wind_speed', Integer),
                Column('wind_direction', String(3)), # 000 to 360
                Column('wind_direction_cardinal', String(3)), # e.g. NNE, NNW
                Column('wind_gust', Integer),
                Column('station_pressure', Float),
                Column('sealevel_pressure', Float),
                Column('precip_1hr', Float, index=True),
                Column('precip_3hr', Float, index=True),
                Column('precip_6hr', Float, index=True),
                Column('precip_24hr', Float, index=True),
                Column('longitude', Float),
                Column('latitude', Float),
                keep_existing=True)

    
    def _extract_last_fname(self):
        # XX: tar files are all old and not recent.
        #tar_last = 
        #tar_last = datetime(2007, 5, 1, 0, 0)
        #tar_filename = '%s.tar.gz' % tar_last.strftime('%Y%m') 
        #print 'tar_filename'

        zip_last = datetime.now()
        self.current_year = zip_last.year
        self.current_month = zip_last.month
        zip_filename = 'QCLCD%s.zip' % zip_last.strftime('%Y%m') 
        return zip_filename

    def _extract_fname(self, year_num, month_num):
        self.current_year = year_num
        self.current_month = month_num
        curr_dt = datetime(year_num, month_num, 1, 0, 0)
        if ((year_num < 2007) or (year_num == 2007 and month_num < 5)):
            tar_filename =  '%s.tar.gz' % (curr_dt.strftime('%Y%m'))
            return tar_filename
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
        return tar_filenames + zip_filenames

    def _load_hourly(self, transformed_input):
        if (self.debug==True):
            transformed_input.seek(0) 
            f = open(os.path.join(self.data_dir, 'weather_etl_dump_hourly.txt'), 'w')
            f.write(transformed_input.getvalue())
            f.close()
        transformed_input.seek(0)
        self.src_hourly_table = self._get_hourly_table(name='src')
        self.src_hourly_table.drop(engine, checkfirst=True)
        self.src_hourly_table.create(engine, checkfirst=True)

        skip_cols = ['id', 'latitude', 'longitude']
        names = [c.name for c in self.hourly_table.columns if c.name not in skip_cols]
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
        if (self.debug==True): 
            self.debug_outfile.write("\nCalling: '%s'\n" % ins_st)
            self.debug_outfile.flush()
        cursor.copy_expert(ins_st, transformed_input)

        conn.commit()
        if (self.debug==True):
            self.debug_outfile.write("Committed: '%s'" % ins_st)
            self.debug_outfile.flush()


    def _load_daily(self, transformed_input): 
        if (self.debug==True):
            transformed_input.seek(0) 
            f = open(os.path.join(self.data_dir, 'weather_etl_dump_daily.txt'), 'w')
            f.write(transformed_input.getvalue())
            f.close()
        transformed_input.seek(0)

        skip_cols = ['id', 'latitude', 'longitude']
        names = [c.name for c in self.daily_table.columns if c.name not in skip_cols]
        self.src_daily_table = self._get_daily_table(name='src')
        self.src_daily_table.drop(engine, checkfirst=True)
        self.src_daily_table.create(engine, checkfirst=True)
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
        if (self.debug==True): 
            self.debug_outfile.write("\nCalling: '%s'\n" % ins_st)
            self.debug_outfile.flush()
        cursor.copy_expert(ins_st, transformed_input)

        conn.commit()
        if (self.debug == True):
            self.debug_outfile.write("committed: '%s'" % ins_st)
            self.debug_outfile.flush()

    def _load_metar(self, transformed_input):
        transformed_input.seek(0)
        #print "_load_metar(): transformed_input is ", transformed_input.getvalue()
        transformed_input.seek(0)
        skip_cols = ['id', 'latitude', 'longitude']
        names = [c.name for c in self.metar_table.columns if c.name not in skip_cols]
        self.src_metar_table = self._get_metar_table(name='src')
        self.src_metar_table.drop(engine, checkfirst=True)

        try:
            self.src_metar_table.create(engine, checkfirst=True)
        except sqlalchemy.exc.ProgrammingError:
            print "got ProgrammingError on src metar table create"
            return None

        
            
        ins_st = "COPY src_weather_observations_metar ("
        for idx, name in enumerate(names):
            if idx < len(names) - 1:
                ins_st += '%s, ' % name
            else:
                ins_st += '%s)' % name
        else:
            ins_st += "FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')"
        #print "_load_metar() ins_st = ", ins_st
        #print "transformed_input is", transformed_input.getvalue()
        conn = engine.raw_connection()
        cursor = conn.cursor()
        if (self.debug==True): 
            self.debug_outfile.write("\nCalling: '%s'\n" % ins_st)
            self.debug_outfile.flush()
        cursor.copy_expert(ins_st, transformed_input)

        conn.commit()
        if (self.debug == True):
            self.debug_outfile.write("committed: '%s'" % ins_st)
            self.debug_outfile.flush()
        
        pass

            
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

    def _get_distinct_weather_stations_by_month(self,year, month, daily_or_hourly='daily'):
        table = Table('dat_weather_observations_%s' % daily_or_hourly, Base.metadata, autoload=True, autoload_with=engine)
        column = None
        if (daily_or_hourly == 'daily'):
            column = table.c.date
        elif (daily_or_hourly == 'hourly'):
            column = table.c.datetime
        
        dt = datetime(year, month,01)
        dt_nextmonth = dt + relativedelta.relativedelta(months=1)
        
        q = session.query(distinct(table.c.wban_code)).filter(and_(column >= dt,
                                                                   column < dt_nextmonth))
        
        station_list = map(operator.itemgetter(0), q.all())
        return station_list

    # Given that this was the most recent month, year, call this function,
    # which will figure out the most recent hourly weather observation and
    # delete all metars before that datetime.
    def clear_metars(self, lastYear, lastMonth, weather_stations_list):
        # build a datetime and then remove all metars after the max datetime
        date_1st = datetime(year=lastYear, month=lastMonth, day=1)
        date_1st_str = datetime.strftime(date_1st, "%Y-%m-%d %H:%M:%S")        
        conn = engine.contextual_connect()
        for wban in weather_stations_list:
            sql = "SELECT wban_code, max (datetime) from dat_weather_observations_hourly where wban_code='%s' AND datetime >= '%s' GROUP BY wban_code" % (wban, date_1st_str)
            # given this time, delete all from dat_weather_observations_metar
            #
            print "executing: ", sql
            results = conn.execute(sql)
            res = results.fetchone()
            if not res:
                continue
            res_dt = res[1]
            res_dt_str = datetime.strftime(res_dt, "%Y-%m-%d %H:%M:%S")        
            # given this most recent time, delete any metars from before that time
            sql2 = "DELETE FROM dat_weather_observations_metar WHERE wban_code='%s' AND datetime < '%s'" % (wban, res_dt_str)
            print "executing: " , sql2
            results = conn.execute(sql2)
    
class WeatherStationsETL(object):
    """ 
    Download, transform and create table with info about weather stations
    """

    def __init__(self):
        self.stations_ftp = \
            'ftp.ncdc.noaa.gov'
        self.stations_file = \
            '/pub/data/noaa/isd-history.csv'

    def initialize(self):
        self._extract()
        self._transform()
        self._make_station_table()
        try:
            self._load()
        except:
            print 'weather stations already exist, updating instead'
            self._update_stations()

    def update(self):
        self._extract()
        self._transform()
        # Doing this just so self.station_table is defined
        self._make_station_table()
        self._update_stations()

    def _extract(self):
        """ Download CSV of station info from NOAA """

        try:
            ftp = FTP(self.stations_ftp) 
            ftp.login()
            stations = StringIO()
            ftp.retrbinary('RETR %s' % self.stations_file, stations.write)
            self.station_raw_info = stations
            self.station_raw_info.seek(0)
        except:
            self.station_info = None
            raise WeatherError('Unable to fetch station data from NOAA.')

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
            wban = row[1]
            name = row[2]
            country = row[3]
            state = row[4]
            call_sign = ''
            lat = row[6].replace('+', '')
            lon = row[7].replace('+', '')
            elev = row[8].replace('+', '')
            begin = parser.parse(row[9]).isoformat()
            end = parser.parse(row[10]).isoformat()
            
            if wban == '99999':
                continue
            elif wban in wbans:
                continue
            elif lat and lon:
                location = 'SRID=4326;POINT(%s %s)' % (lon, lat)
                wbans.append(wban)
                all_rows.append([wban, name, country, state, 
                    call_sign, location, elev, begin, end])
        writer = UnicodeCSVWriter(self.clean_station_info)
        writer.writerow(header)
        writer.writerows(all_rows)
        self.clean_station_info.seek(0)

    def _make_station_table(self):
        self.station_table = Table('weather_stations', Base.metadata,
                Column('wban_code', String(5), primary_key=True),
                Column('station_name', String(100), nullable=False),
                Column('country', String(2)),
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


        
        
