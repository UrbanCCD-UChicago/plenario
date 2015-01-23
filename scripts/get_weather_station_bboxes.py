import sys
import time
import requests
import json
from datetime import date, datetime, timedelta
from dateutil import parser
from shapely.geometry import box, asShape
import math

from datetime import datetime
from dateutil import relativedelta
from plenario.utils import weather

###############################################################################
# SCRIPT: get_weather_station_bboxes.py
#
# DESCR: This script, given a Plenario instance (defined in base_url below),
# uses the API to iterate over all datasets and find all the weather
# stations within their bounding box (with a rectangular buffer of 25 miles).
#
# When run standalone, this will print all within-bbox weather stations
# out in a convenient csv format.
###############################################################################

#base_url = 'http://plenar.io'
base_url = 'http://127.0.0.1:5001'

weather_etl = weather.WeatherETL(debug=True)

def json_pp(resp):
    return json.dumps(resp, indent=4,  separators=(',', ': '))

def my_fetch_url(url):
    try:
        start = time.time()
        r = requests.get(url)
        end = time.time()
        diff = end - start
        try:
            resp = r.json()
            return resp, diff, url
        except ValueError:
            # print 'Junk response'
            return None, None, url
    except requests.exceptions.Timeout, e:
        print 'Request timeout %s' % url
        return None, None, url
    except requests.exceptions.ConnectionError, e:
        print 'Connection reset: %s' % url
        return None, None, url

def get_all_datasets():
    url = "%s/v1/api/datasets/" % base_url
    (resp, time_val, url) = my_fetch_url(url)
    return resp

def get_all_bboxes():
    bbox_list = []
    resp = get_all_datasets()
    for r in resp['objects']:
        bbox_list.append((r['human_name'], r['source_url_hash'], r['bbox']))
    return bbox_list

def get_weather_stations_in_bbox(bbox_dict):
    # given a (json?) bbox do a query
    bbox_json_str=  json.dumps(bbox_dict)
    url = '%s/v1/api/weather-stations?location__within=%s' % (base_url, bbox_json_str)
    (resp, time_val, url) = my_fetch_url(url)
    return resp

def get_expanded_bbox(bbox):
    buff = 40234 # in meters
    shape= asShape(bbox)
    #print "shape is ", shape
    lat = shape.centroid.y
    # 25 miles = 40233.6 meters by default
    x, y = getSizeInDegrees(int(buff), lat)
    #size_x, size_y = getSizeInDegrees(40234, lat)
    location_geom = shape.buffer(y).__geo_interface__
    #print "location_geom=", location_geom
    return location_geom
    
def getSizeInDegrees(meters, latitude):
    earth_circumference = 40041000.0 # meters, average circumference
    degrees_per_meter = 360.0 / earth_circumference
    degrees_at_equator = meters * degrees_per_meter
    latitude_correction = 1.0 / math.cos(latitude * (math.pi / 180.0))
    degrees_x = degrees_at_equator * latitude_correction
    degrees_y = degrees_at_equator
    return degrees_x, degrees_y

def insert_data_in_month(start_month, start_year, end_month, end_year, no_daily=False, no_hourly=False, weather_stations_list = None, debug=False):
    month = start_month
    for year in range(start_year, end_year +1):
        while (month <= 12):
            if (debug):
                print "\n"
                print "==== insert_data_in_month(", start_month, start_year, end_month, end_year, debug," )"

            dt = datetime(year, month,01)
            dt_nextmonth = dt + relativedelta.relativedelta(months=1)

            # get the list of stations for this month
            #curr_weather_stations = weather_etl._get_distinct_weather_stations_by_month(year, month)

            print "weather_etl.initialize_month(",year,month,True,")"
            weather_etl.initialize_month(year,month,no_daily=no_daily,no_hourly=no_hourly, weather_stations_list=weather_stations_list)
            #, banned_weather_stations_list=curr_weather_stations)
            
            if (year==end_year and (month + 1)>end_month):
                return
            month += 1
        month  = 1

if __name__=="__main__":
    total_wbans = set()
    wban_dict = {}
    source_dict = {}
    bboxes = get_all_bboxes()
    
    for (human_name, source_url_hash, bbox) in bboxes:
        if (bbox is not None):

            expanded_bbox = get_expanded_bbox(bbox)
            weather_stations_retval = get_weather_stations_in_bbox(expanded_bbox)
            weather_stations = weather_stations_retval['objects']

            for ws in weather_stations:
                wban = ws['wban_code']
                total_wbans.update([wban])
                if wban not in wban_dict:
                    wban_dict[wban] = ws
                    source_dict[wban] = (source_url_hash, human_name)
            # Now we can, e.g., import hourly data just for those weather_stations.
            
    # Dump this list of wbans as csv to stdout for later processing.
    print "state,station_name,wban_code,lng,lat,source_url_hash,human_name"
    for ws in wban_dict.values():
        print "%s,%s,%s,%f,%f,%s,\"%s\"" % (ws['state'], ws['station_name'], ws['wban_code'], ws['location']['coordinates'][0], ws['location']['coordinates'][1], source_dict[ws['wban_code']][0], source_dict[ws['wban_code']][1])
