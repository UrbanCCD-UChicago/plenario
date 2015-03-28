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
from math import sin
from math import radians as rad

sys.path.append('.')
import plenario
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

base_url = 'http://plenar.io'
#base_url = 'http://127.0.0.1:5001'

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
    print "url is" , url
    (resp, time_val, url) = my_fetch_url(url)
    #print "get_all_datasets(): length(resp['objects'])=", len(resp['objects'])
    #for o in resp['objects']:
    #    o['dataset_name']
    #sys.exit(1)
    return resp

def get_all_bboxes(whitelist_urls, blacklist_urls):
    bbox_list = []
    resp = get_all_datasets()
    for r in resp['objects']:
        url = r['source_url_hash']
        if url not in whitelist_urls and whitelist_urls != []:
            continue
        if url in blacklist_urls:
            continue
        bbox_list.append((r['human_name'], r['source_url_hash'], r['bbox'] ,r['attribution']))
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


def get_area_from_latlng(x1,y1,x2,y2,x3,y3,x4,y4,x5,y5):
    area = rad(x2 - x1) * (2 + sin(rad(y1)) + sin(rad(y2))) + rad(x3 - x2) * (2 + sin(rad(y2)) + sin(rad(y3))) + rad(x4 - x3) * (2 + sin(rad(y3)) + sin(rad(y4))) + rad(x5 - x4) * (2 + sin(rad(y4)) + sin(rad(y5)))

    area = abs(area * 6378137.0 * 6378137.0 / 2.0)
    print "area is" , area, ". sq miles=" , area/38610200.



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

def latlngs_to_sq_meters(lat1,lng1,lat2,lng2):
    area = None
    return area
        
# make it so that if you pass it a particular dataset, it gives you the bounding box for it
        
if __name__=="__main__":
    total_wbans = set()
    wban_dict = {}
    source_dict = {}
    source_name_dict = {}

    blacklist_urls = ['63c2891fb31af035eb97ae9fbc62112e', # New York State Office of Children and Family Services, Child Care Regulated Programs
                   '012fa744cb547a937d0b508c04153531',    # State of Illinois , IEMA Dental Facilities in Illinois
                   'c1bc95fcfed180ef92d70bda7e377ba6',    # State of Illinois , IEMA Non-Dental Facilities in Illinois with Radiation Producing Equipment"
                   'c3f37146922c5aa09c071c3cc045112f',    # "San Francisco Police - Crime Incidents"
                   'cac580c8c6799995b220300a584090f6',    # New York State Liquor Authority ,Liquor Authority Quarterly List of Active Licenses
                   '601864623cc113e5351f1d0be9bdb36e',    # "State University Construction Fund (SUCF) Contracts: Beginning 1995"
                   'ceed1a0def5dd83cc0e16c4e807d284d',    # City of Rockford , City of Rockford graffiti abatement requests, Graffiti (points in Iowa)
                   'eb5e0a350b0f78a60de36e6fd13c4da7',    # City of Rockford , City of Rockford graffiti abatement requests, Graffiti (points in Iowa)
                   '08df6cf6608f28932c0e0ac050f1a1bd',    # New York State Liquor Authorit , Liquor Authority Quarterly List of Active Licenses API
                   'b8a9b9e6bf036ba5ffdf28738d082e3c'     # City of Austin, 311 Unified Data (random points in Pacific Ocean south of Mexico)
    ]

    #whitelist_urls = ['ce29323c565cbd4a97eb61c73426fb01']
    #whitelist_urls = ['ce29323c565cbd4a97eb61c73426fb01']
    whitelist_urls = []
    
    bboxes = get_all_bboxes(whitelist_urls, blacklist_urls)

    for (human_name, source_url_hash, bbox, attribution) in bboxes:
        if source_url_hash not in whitelist_urls and whitelist_urls != []:
            continue
        
        if source_url_hash in blacklist_urls:
            continue
        
        if (bbox is not None):

            curr_bbox = bbox['coordinates'][0]
            #print human_name, "bbox is" , curr_bbox
            print '"%s","%s","%s",%f,%f,%f,%f,%f,%f,%f,%f' % (source_url_hash,
                                                              attribution,
                                                              human_name,
                                                              curr_bbox[0][0],
                                                              curr_bbox[0][1],
                                                              curr_bbox[1][0],
                                                              curr_bbox[1][1],
                                                              curr_bbox[2][0],
                                                              curr_bbox[2][1],
                                                              curr_bbox[3][0],
                                                              curr_bbox[3][1])
            
            get_area_from_latlng(curr_bbox[0][0],
                                 curr_bbox[0][1],
                                 curr_bbox[1][0],
                                 curr_bbox[1][1],
                                 curr_bbox[2][0],
                                 curr_bbox[2][1],
                                 curr_bbox[3][0],
                                 curr_bbox[3][1],
                                 curr_bbox[0][0],
                                 curr_bbox[0][1])
                                 
            
            expanded_bbox = get_expanded_bbox(bbox)
            weather_stations_retval = get_weather_stations_in_bbox(expanded_bbox)
            weather_stations = weather_stations_retval['objects']

            for ws in weather_stations:
                wban = ws['wban_code']
                total_wbans.update([wban])
                if wban not in wban_dict:
                    wban_dict[wban] = ws
                #source_dict[wban] = (source_url_hash, human_name)
                if source_url_hash not in source_name_dict:
                    source_name_dict[source_url_hash] = human_name
                if source_url_hash not in source_dict:
                    source_dict[source_url_hash] = [wban]
                else:
                    source_dict[source_url_hash].append(wban)

            # Now we can, e.g., import hourly data just for those weather_stations.
            
    # Dump this list of wbans as csv to stdout for later processing.
    #print "state,station_name,wban_code,lng,lat,source_url_hash,human_name"
    #for ws in wban_dict.values():
        #print "%s,%s,%s,%f,%f"  % (ws['state'], ws['station_name'], ws['wban_code'], ws['location']['coordinates'][0], ws['location']['coordinates'][1])

    print "hash,human_name,wbans"
    for source_url_hash in source_dict.keys():
        print '%s,"%s","%s"' % (source_url_hash, source_name_dict[source_url_hash],str(source_dict[source_url_hash]))
        for s in source_dict[source_url_hash]:
            ws = wban_dict[s]
            print "%s,%s,%s,%f,%f"  % (ws['state'], ws['station_name'], ws['wban_code'], ws['location']['coordinates'][0], ws['location']['coordinates'][1])
        
    print '"Final list of WBANS:"'
    wlist = []
    for ws in wban_dict.values():
        wlist.append(ws['wban_code'])

    print wlist
