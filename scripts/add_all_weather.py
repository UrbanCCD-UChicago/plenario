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
    wban_list = [u'12921', u'94740', u'14855', u'03981', u'93894', u'94746', u'93816', u'93904', u'03887', u'14752', u'93810', u'53981', u'04953', u'14776', u'14777', u'53983', u'54723', u'14770', u'14771', u'94765', u'94761', u'93986', u'14778', u'03968', u'53952', u'53950', u'94892', u'94891', u'54757', u'13802', u'23293', u'54734', u'54735', u'54733', u'04789', u'64775', u'04787', u'64776', u'04783', u'54738', u'54739', u'93823', u'93822', u'14740', u'23272', u'53802', u'54811', u'13922', u'03985', u'14742', u'03972', u'04868', u'14747', u'53822', u'93227', u'93817', u'93228', u'14748', u'04867', u'04866', u'03879', u'23289', u'53891', u'04862', u'53944', u'53947', u'64705', u'53942', u'64707', u'23203', u'53935', u'54704', u'63853', u'94982', u'14834', u'64761', u'04751', u'04876', u'04871', u'04873', u'04739', u'04879', u'93232', u'93231', u'14758', u'13902', u'14754', u'23240', u'94741', u'14757', u'14750', u'94979', u'94745', u'14753', u'53939', u'12910', u'03868', u'14923', u'03902', u'53933', u'54831', u'94870', u'13904', u'14775', u'64758', u'13911', u'04996', u'64753', u'12911', u'63840', u'63841', u'64757', u'64756', u'13909', u'04903', u'23250', u'23257', u'04807', u'23254', u'04724', u'04808', u'04726', u'23258', u'04720', u'03999', u'13984', u'94908', u'13986', u'23285', u'04889', u'14931', u'54789', u'54788', u'64706', u'94868', u'14733', u'94866', u'54782', u'54781', u'03948', u'54787', u'54786', u'54785', u'03919', u'14719', u'54768', u'54760', u'04925', u'93997', u'54767', u'94721', u'94723', u'94725', u'03969', u'53999', u'04725', u'23244', u'94728', u'12961', u'23907', u'14739', u'53913', u'54792', u'54793', u'14732', u'93211', u'04950', u'14736', u'14737', u'14734', u'14735', u'04899', u'53938', u'04890', u'03928', u'04896', u'04894', u'94854', u'53997', u'94822', u'54778', u'54779', u'13966', u'13967', u'04949', u'14786', u'54770', u'94959', u'54777', u'23239', u'94733', u'93943', u'04947', u'14703', u'14702', u'12979', u'04742', u'23230', u'04741', u'14990', u'23234', u'53986', u'12971', u'94846', u'23237', u'03932', u'04930', u'53909', u'53897', u'54808', u'13919', u'14816', u'03838', u'14819', u'03933', u'63814', u'63817', u'63810', u'53886', u'13958', u'14790', u'14792', u'54756', u'14794', u'93942', u'54790', u'04838', u'93978', u'14712', u'13961', u'14714', u'14715', u'14717', u'63878', u'04831', u'94705', u'94704', u'94702', u'54773', u'13999', u'14880', u'93947', u'03958', u'94789', u'93984', u'93985', u'53977', u'53976', u'64774', u'53979', u'93989', u'54742', u'53150', u'23211', u'13940', u'14937', u'13945', u'04921', u'14842', u'53887', u'04880', u'53889', u'54771', u'04781', u'94794', u'94790', u'14707', u'54780', u'14761', u'14760', u'14763', u'14762', u'04845', u'14768', u'54746', u'53964', u'53965', u'54743', u'63901', u'54740', u'13809', u'03950', u'53969', u'03957', u'03954', u'94737']
    # If you uncomment the below, all daily weather will be added up through 2014.
    #insert_data_in_month(7,1996, 12,2014, no_hourly=True, debug=True)
    insert_data_in_month(7,1996, 12,2014, no_daily=True, debug=True, weather_stations_list = wban_list)
    pass

