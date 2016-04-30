from metar.metar import Metar
from plenario.database import app_engine as engine
import requests
import csv

from lxml import etree
from lxml.etree import fromstring
from lxml import objectify

# Example METAR URL: 'https://aviationweather.gov/adds/dataserver_current/httpparam?datasource=metars&requesttype=retrieve&format=xml&hoursBeforeNow=1.25&stationString=KORD'

current_METAR_url = 'http://aviationweather.gov/adds/dataserver_current/current/'


def _make_call_sign_wban_map():
    with open('plenario/utils/wban_to_call_sign.csv') as fp:
        reader = csv.reader(fp)
        # Discard header
        reader.next()
        call_sign_to_wban_map = {row[1]: row[0] for row in reader}
    return call_sign_to_wban_map

call_sign_wban_map = _make_call_sign_wban_map()

# An example code:
# - In this example, we have "few clouds at 1500 feet, broken clouds at 4,000 feet w/ cumulonimbus,
#   broken at 6,500 feet, overcast at 20,000 feet"
# - Visibility is 2 statute miles
#code = "METAR KEWR 111851Z VRB03G19KT 2SM R04R/3000VP6000FT TSRA BR FEW015 BKN040CB BKN065 OVC200 22/22 A2987 RMK AO2 PK WND 29028/1817 WSHFT 1812 TSB05RAB22 SLP114 FRQ LTGICCCCG TS OHD AND NW-N-E MOV NE P0013 T02270215"

def getMetar(metar_string):
    m =  Metar(metar_string)
    return m

def all_callSigns():
    sql = "SELECT call_sign FROM weather_stations ORDER by call_sign"
    result=engine.execute(sql)
    return [x[0] for x in result.fetchall()]


def callSign2Wban(call_sign):
    return call_sign_wban_map.get(call_sign)


def wban2CallSign(wban_code):
    sql = "SELECT call_sign FROM weather_stations where wban_code='%s'" % wban_code
    result = engine.execute(sql)
    #print "result=", result
    x = result.first()
    cs = None
    if x:
        cs = x[0]
        #print "wban=", wban
    else:
        print "could not find wban:", wban_code
    return cs
        

def getCurrentWeather(call_signs=None, wban_codes=None, all_stations=False, wban2callsigns=None):
    xml_METAR_url = 'http://aviationweather.gov/adds/dataserver_current/httpparam?datasource=metars&requesttype=retrieve&format=xml&hoursBeforeNow=1.25'
    # Example of multiple stations: https://aviationweather.gov/adds/dataserver_current/httpparam?datasource=metars&requesttype=retrieve&format=xml&hoursBeforeNow=1.25&stationString=KORD,KMDW

    if (all_stations == True):
        # We should grab our list from weather_stations and only ask for 100 at a time. 
        #print "all_callSigns is ", all_callSigns()
        #print "len(all_callSigns) is ", len(all_callSigns())
        # XXXXXX TOOO
        pass
    elif (call_signs and wban_codes):
        print "error: define only call_signs or wban_codes and not both"
    elif (wban_codes):
        # Convert all wban_codes to call_signs
        if (wban2callsigns):
            call_signs = []
            for wban in wban_codes:
                if wban in wban2callsigns:
                    call_signs.append(wban2callsigns[wban])
        else:
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
        # XXXXXX: doing all stations
        pass
    
    print "xml_METAR_url: '%s'" % xml_METAR_url
    return raw_metars_from_url(xml_METAR_url)


def raw_metars_from_url(url):
    req = requests.get(url)
    xml = req.text

    xml_u = xml.encode('utf-8')

    parser = etree.XMLParser(ns_clean=True, recover=True, encoding='utf-8')
    h = fromstring(xml_u, parser=parser)
    #tree = etree.parse(StringIO(xml_u))
    root = objectify.fromstring(xml_u)
    #print "root is ", root, type(root)

    metars = root['data']['METAR']

    metar_raws = []
    for m in metars:
        metar_raw = m['raw_text'].text
        metar_raws.append(metar_raw)

    print "completed len(metar_raws)= %d" % len(metar_raws)
    return metar_raws


def getAllCurrentWeather():
    all_metar_url = 'http://aviationweather.gov/adds/dataserver_current/current/metars.cache.xml'
    return raw_metars_from_url(all_metar_url)
    # all_calls = all_callSigns()
    # all_metars = []
    # for i in range(0, len(all_calls), 1000):
    #     calls_range = all_calls[i:(i+1000)]
    #     metars = getCurrentWeather(call_signs=calls_range)
    #     all_metars.extend(metars)
    #
    # print "getAllCurrentWeather(): total metar collection is length", len(all_metars)



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
        #print "sky_cond, height, detail=",sky_cond, height, detail
        if height:
            height_100s_feet = height.value() / 100.00
        else:
            height_100s_feet = None
        sky_str = None
        if detail:
            try:
                sky_str = '%s%03d%s' % (sky_cond, height_100s_feet, detail)
            except TypeError, e:
                print "parsing error on ", (sky_cond, height, detail), e
        elif height:
            sky_str = '%s%03d' % (sky_cond, height_100s_feet)
        else:
            sky_str = '%s' % sky_cond
        if sky_str:
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
    from weather import degToCardinal
    wind_speed = None
    wind_speed_int = None
    wind_direction = None
    wind_direction_int = None
    wind_direction_cardinal = None
    wind_gust = None
    wind_gust_int = None

    if (obs.wind_speed):
        wind_speed = obs.wind_speed.value(units="MPH")
        wind_speed_int = int(round(float(wind_speed)))
    if (obs.wind_dir):
        wind_direction = obs.wind_dir.value()
        wind_direction_int = int(round(float(wind_direction)))
        wind_direction_cardinal = degToCardinal(wind_direction_int)
    if (obs.wind_gust):
        wind_gust = obs.wind_gust.value()    
        wind_gust_int = int(round(float(wind_gust)))
    
    return wind_speed_int, wind_direction_int, wind_direction_cardinal, wind_gust_int

def getPressure(obs):
    pressure_in = None
    if (obs.press):
        pressure_in = obs.press.value(units="IN")
    return pressure_in

def getPressureSeaLevel(obs):
    pressure_in = None
    if (obs.press_sea_level):
        pressure_in = obs.press_sea_level.value(units="IN")
    return pressure_in

def getPrecip(obs):
    precip_1hr = None
    precip_3hr = None
    precip_6hr = None
    precip_24hr = None

    if obs.precip_1hr:
        precip_1hr = obs.precip_1hr.value()
    if obs.precip_3hr:
        precip_3hr = obs.precip_3hr.value()
    if obs.precip_6hr:
        precip_6hr = obs.precip_6hr.value()
    if obs.precip_24hr:
        precip_24hr = obs.precip_24hr.value()

    return precip_1hr, precip_3hr, precip_6hr, precip_24hr
        

def dumpMetar(metar):
    pass

def getMetarVals(metar):
    wban_code = getWban(metar)
    call_sign = metar.station_id
    datetime = metar.time
    sky_condition, sky_condition_top = getSkyCondition(metar)
    visibility = getVisibility(metar)
    weather_types = getWeatherTypes(metar)
    f = getTempFahrenheit(metar)
    dp = getDewpointFahrenheit(metar)
    wind_speed, wind_direction_int, wind_direction_cardinal, wind_gust = getWind(metar)
    pressure = getPressure(metar) 
    pressure_sea_level = getPressureSeaLevel(metar) 
    # XXX do snow depth ("Usually found in the 06 and 18Z observations.")
    # (XXX: snow depth not found in current metar parse, but could be wrong.)
    precip_1hr, precip_3hr, precip_6hr, precip_24hr = getPrecip(metar)
    
    #print "wban: ", wban_code
    #print "datetime: ", datetime
    #print "sky_condition: ", sky_condition
    #print "sky_condition_top: ", sky_condition_top
    #print "weather_types: ", weather_types
    #print "temp: " , f, "F"
    #print "dewpoint: ", dp, "F"
    #print "wind speed:", wind_speed, "MPH", "wind_direction: ", wind_direction_int, "wind_direction_cardinal:", wind_direction_cardinal
    #print "pressure: ", pressure, "IN"
    #print "pressure (sea_level): ", pressure_sea_level, "IN"
    #print "precip (1hr, 3hr, 6hr, 24hr):", precip_1hr, precip_3hr, precip_6hr, precip_24hr

    return [wban_code, call_sign, datetime, sky_condition, sky_condition_top,
            visibility, weather_types, f, dp,
            wind_speed, wind_direction_int, wind_direction_cardinal, wind_gust,
            pressure, pressure_sea_level,
            precip_1hr, precip_3hr, precip_6hr, precip_24hr]
    
def dumpRawMetar(raw_metar):
    print "raw_metar=", raw_metar
    obs = Metar(raw_metar)
    dumpMetar(obs)

##allw = getAllCurrentWeather()

# here's a list of Illinois-area wban_codes (from python scripts/get_weather_station_bboxes.py where whitelist_urls=['ce29323c565cbd4a97eb61c73426fb01']
illinois_area_wbans = [u'14855', u'54808', u'14834', u'04838', u'04876', u'03887', u'04871', u'04873', u'04831', u'04879', u'04996', u'14880', u'04899', u'94892', u'94891', u'04890', u'54831', u'94870', u'04894', u'94854', u'14842', u'93822', u'04807', u'04808', u'54811', u'94822', u'94846', u'04868', u'04845', u'04896', u'04867', u'04866', u'04889', u'14816', u'04862', u'94866', u'04880', u'14819']

##illinois_w = getCurrentWeather(wban_codes=illinois_wbans)
#metars = []
#om = None
#rain_metar = None
#all_metars = []
#for w in allw:
##for w in illinois_w:
#    try:
#        metar = Metar(w)
#    except ParserError, e:
#        print "parser error! on error" , e
#
#    all_metars.append(metar)
#    wban = getWban(metar)
#    if (wban in illinois_wbans):
#        print w
#        if (wban =='94846'):
#            om = metar # ohare metar
#        if metar.precip_1hr:
#            rain_metar = metar
#        print "got Wban from metar: ", wban
#        
#        metars.append(metar)
#    #print "metar is", metar
#    dumpMetar(metar)
#    
#for obs in metars:
#    #print obs.weather
#    pass
