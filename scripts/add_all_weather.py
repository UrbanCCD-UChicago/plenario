from plenario.utils import weather
from plenario.database import task_session as session, task_engine as engine, Base
from sqlalchemy import Table, select, func, and_, distinct
from sqlalchemy import event
from sqlalchemy.engine import Engine
import time
import sys
import operator

from datetime import datetime
from dateutil import relativedelta

from plenario.utils import weather

###############################################################################
# SCRIPT: add_all_weather.py
#
# DESCR: This script, using a local Plenario instance, calls
# initialize_month() for a range of months to add daily OR hourly QCLCD weather data.
#  - Pass no_hourly=True to only do daily datasets.
#  - Pass no_daily=True to only do hourly datasets.
#  - Pass a list of WBANs in weather_stations_list to only insert particular weather stations.
###############################################################################

weather_etl = weather.WeatherETL(debug=True)

def insert_data_in_month(start_month, start_year, end_month, end_year, no_daily=False, no_hourly=False, weather_stations_list = None, debug=False):
    month = start_month
    for year in range(start_year, end_year +1):
        while (month <= 12):
            if (debug):
                print "\n"
                print "==== insert_data_in_month(", start_month, start_year, end_month, end_year, debug," )"

            dt = datetime(year, month,01)
            dt_nextmonth = dt + relativedelta.relativedelta(months=1)

            print "weather_etl.initialize_month(",year,month,True,")"
            weather_etl.initialize_month(year,month,no_daily=no_daily,no_hourly=no_hourly, weather_stations_list=weather_stations_list)
            
            if (year==end_year and (month + 1)>end_month):
                return
            month += 1
        month  = 1

if __name__=="__main__":
    # If you uncomment the below, all daily weather will be added up through 2014.
    #insert_data_in_month(7,1996, 12,2014, no_hourly=True, debug=True)
    pass

