import plenario
from plenario.utils import weather
from plenario.utils.weather import WeatherETL, WeatherStationsETL
import metar
from metar.metar import Metar, ParserError
from plenario.database import task_session as session, task_engine as engine, Base
from sqlalchemy import Table, select, func, and_, distinct
from sqlalchemy import event
from sqlalchemy.engine import Engine
import sys
import grequests
from plenario.utils.weather import degToCardinal

import pdb
from lxml import etree
from lxml.etree import fromstring
from cStringIO import StringIO
from lxml import objectify

current_METAR_url = 'https://aviationweather.gov/adds/dataserver_current/current/'

#xml_METAR_url = 'https://aviationweather.gov/adds/dataserver_current/httpparam?datasource=metars&requesttype=retrieve&format=xml&hoursBeforeNow=1.25&stationString=KORD'

weather_etl = weather.WeatherETL(debug=True)

# An example code:
# - In this example, we have "few clouds at 1500 feet, broken clouds at 4,000 feet w/ cumulonimbus,
#   broken at 6,500 feet, overcast at 20,000 feet"
# - Visibility is 2 statute miles
#code = "METAR KEWR 111851Z VRB03G19KT 2SM R04R/3000VP6000FT TSRA BR FEW015 BKN040CB BKN065 OVC200 22/22 A2987 RMK AO2 PK WND 29028/1817 WSHFT 1812 TSB05RAB22 SLP114 FRQ LTGICCCCG TS OHD AND NW-N-E MOV NE P0013 T02270215"
code = "METAR KEWR 111851Z VRB03G19KT 2SM R04R/3000VP6000FT TSRA BR FEW015 BKN040CB BKN065 OVC200 22/22 A2987 RMK AO2 PK WND 29028/1817 WSHFT 1812 TSB05RAB22 SLP114 FRQ LTGICCCCG TS OHD AND NW-N-E MOV NE P0013 T02270215"

# using the python-metar library
obs = Metar(code)

# what we want to get out of this is something like the dat_weather_observations_hourly table:
#wban_code               | 00102
#datetime                | 2014-09-01 00:01:00
#old_station_type        |
#station_type            | 0
#sky_condition           | OVC015
#sky_condition_top       | OVC015
#visibility              | 10
#weather_types           | {{-,NULL,NULL,RA,NULL,NULL}}
#drybulb_fahrenheit      | 46
#wetbulb_fahrenheit      | 46
#dewpoint_fahrenheit     | 45
#relative_humidity       | 96
#wind_speed              | 8
#wind_direction          | 200
#wind_direction_cardinal | SSW
#station_pressure        | 29.36
#sealevel_pressure       | 29.65
#report_type             | AA
#hourly_precip           |
#longitude               |
#latitude                |
#id                      | 3625751

#ws = weather.WeatherStationsETL()
#wst=Table('weather_stations', Base.metadata, autoload=True, autoload_with=engine)
#foo = wst.select(wst.c.call_sign == obs.station_id)
#result = foo.execute()

def callSign2Wban(call_sign):
    sql = "SELECT wban_code FROM weather_stations where call_sign='%s'" % call_sign
    result = engine.execute(sql)
    print "result=", result
    x = result.first()
    wban = None
    if x:
        wban = x[0]
        #print "wban=", wban
    else:
        print "could not find call sign:", call_sign
    return wban

def wban2CallSign(wban_code):
    sql = "SELECT call_sign FROM weather_stations where wban_code='%s'" % wban_code
    result = engine.execute(sql)
    print "result=", result
    x = result.first()
    cs = None
    if x:
        cs = x[0]
        #print "wban=", wban
    else:
        print "could not find wban:", wban_code
    return cs
        

def getCurrentWeather(call_signs=None, wban_codes=None, all_stations=False):
    xml_METAR_url = 'https://aviationweather.gov/adds/dataserver_current/httpparam?datasource=metars&requesttype=retrieve&format=xml&hoursBeforeNow=1.25'
    # example of multiple stations: https://aviationweather.gov/adds/dataserver_current/httpparam?datasource=metars&requesttype=retrieve&format=xml&hoursBeforeNow=1.25&stationString=KORD,KMDW

    if (all_stations == True):
        pass
    elif (call_signs and wban_codes):
        print "error: define only call_signs or wban_codes and not both"
    elif (wban_codes):
        # convert all wban_codes to call_signs
        call_signs = []
        for wban_code in wban_codes:
            call_sign = wban2CallSign(wban_code)
            if (call_sign):
                call_signs.append(call_sign)

    if (call_signs):
        # OK, we have call signs now
        xml_METAR_url += '&stationString='
        xml_METAR_url += ','.join(map(lambda x:x.upper(), call_signs))
    else:
        # doing all stations
        pass
    
    print "xml_METAR_url:", xml_METAR_url
    req = grequests.get(xml_METAR_url)
    result_list =  grequests.map([req])
    xml = result_list[0].text

    xml_u = xml.encode('utf-8')
    
    parser = etree.XMLParser(ns_clean=True, recover=True, encoding='utf-8')
    h = fromstring(xml_u, parser=parser)
    #tree = etree.parse(StringIO(xml_u))
    root = objectify.fromstring(xml_u)
    print "root is ", root, type(root)

    metars = root['data']['METAR']

    metar_raws = []
    for m in metars:
        metar_raw = m['raw_text'].text
        metar_raws.append(metar_raw)
    
    return metar_raws


def getWban(obs):
    if obs.station_id:
        return callSign2Wban(obs.station_id)
    else:
        return None

def getSkyCondition(obs):
    skies = obs.sky
    #print "skies=", skies
    sky_list =  []
    height_max = 0
    sky_top = None
    sky_str = None
    for (sky_cond, height, detail) in skies:
        print "sky_cond, height, detail=",sky_cond, height, detail
        if height:
            height_100s_feet = height.value() / 100.00
        else:
            height_100s_feet = None
        sky_str = None
        if detail:
            sky_str = '%s%03d%s' % (sky_cond, height_100s_feet, detail)
        elif height:
            sky_str = '%s%03d' % (sky_cond, height_100s_feet)
        else:
            sky_str = '%s' % sky_cond
        sky_list.append(sky_str)
        if (height_100s_feet is not None) and (height_100s_feet > height_max):
            sky_top = sky_str
    if not sky_top:
        # just set the top to the last one we added
        sky_top = sky_str
    sky_str = ' '.join(sky_list)
    return sky_str, sky_top
        
def getVisibility(obs):
    if obs.vis:
        return obs.vis.value()
    else:
        return None

def getWeatherTypes(obs):
    weathers = obs.weather
    if len(weathers) == 0:
        return []

    ret_weather_types= []
    # see: METAR and TAF codes.pdf p. 11
    for [intensityProximity, desc, precip, obscur, other] in weathers:
        finalIntensity =None
        finalProximity = None
        finalDesc = None
        finalPrecip = None
        finalObscur = None
        finalOther = None
        if ((intensityProximity is None) or (len(intensityProximity) == 0)):
            pass
        elif ((intensityProximity[0] == '+') or (intensityProximity[0] == '-')):
            finalIntensity = intensityProximity[0]
            if len(intensityProximity[1:]) == 0:
                finalProximity = None
            else:
                finalProximity=intensityProximity[1:]
        finalDesc = desc
        finalPrecip = precip
        finalObscur = obscur
        finalOther = other
        ret_weather_types.append([finalIntensity, finalProximity, finalDesc, finalPrecip, finalObscur, finalOther])
        
    return ret_weather_types

def getTempFahrenheit(obs):
    if (obs.temp):
        curr_f = obs.temp.value(units='F')
    else:
        curr_f = None
    return curr_f

def getDewpointFahrenheit(obs):
    if (obs.dewpt):
        dp = obs.dewpt.value(units='F')
    else:
        dp = None
    return dp
    
def getWind(obs):
    wind_speed = None
    wind_direction = None
    wind_direction_int = None
    wind_direction_cardinal = None

    if (obs.wind_speed):
        wind_speed = obs.wind_speed.value(units="MPH")
    if (obs.wind_dir):
        wind_direction = obs.wind_dir.value()
        wind_direction_int = int(round(float(wind_direction)))
        wind_direction_cardinal = degToCardinal(wind_direction_int)
    
    return wind_speed, wind_direction_int, wind_direction_cardinal

def getPressure(obs):
    pressure_in = None
    if (obs.press):
        pressure_in = obs.press.value(units="IN")
    return pressure_in

def dumpMetar(metar):
    wban_code = getWban(metar)
    datetime = metar.time
    sky_condition, sky_condition_top = getSkyCondition(metar)
    visibility = getVisibility(metar)
    weather_types = getWeatherTypes(metar)
    f = getTempFahrenheit(metar)
    dp = getDewpointFahrenheit(metar)
    wind_speed, wind_direction_int, wind_direction_cardinal = getWind(metar)
    pressure = getPressure(metar)
    #XXX
    
    print "wban: ", wban_code
    print "datetime: ", datetime
    print "sky_condition: ", sky_condition
    print "sky_condition_top: ", sky_condition_top
    print "weather_types: ", weather_types
    print "temp: " , f, "F"
    print "dewpoint: ", dp, "F"
    print "wind speed:", wind_speed, "MPH", "wind_direction: ", wind_direction_int, "wind_direction_cardinal:", wind_direction_cardinal
    print "pressure: ", pressure, "IN"
    
def dumpRawMetar(raw_metar):
    print "raw_metar=", raw_metar
    obs = Metar(raw_metar)
    dumpMetar(obs)

#kord = getCurrentWeather(['KORD'])
#dumpRawMetar(kord[0])
allw = getCurrentWeather(None,None,all_stations = True)

# here's a list of Illinois-area wban_codes (from python scripts/get_weather_station_bboxes.py where whitelist_urls=['ce29323c565cbd4a97eb61c73426fb01'] (idol_public_works_prohibited_contractors)

illinois_wbans = [u'14855', u'54808', u'14834', u'04838', u'04876', u'03887', u'04871', u'04873', u'04831', u'04879', u'04996', u'14880', u'04899', u'94892', u'94891', u'04890', u'54831', u'94870', u'04894', u'94854', u'14842', u'93822', u'04807', u'04808', u'54811', u'94822', u'94846', u'04868', u'04845', u'04896', u'04867', u'04866', u'04889', u'14816', u'04862', u'94866', u'04880', u'14819']

#illinois_w = getCurrentWeather(wban_codes=illinois_wbans)

metars = []
for w in allw:
#for w in illinois_w:
    print w
    try:
        metar = Metar(w)
    except ParserError, e:
        print "parser error! on error" , e
    metars.append(metar)
    #print "metar is", metar
    dumpMetar(metar)

    
for obs in metars:
    #print obs.weather
    pass
