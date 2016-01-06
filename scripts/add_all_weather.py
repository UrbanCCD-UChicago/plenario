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
    wban_list = ['12921', '94740', '14855', '03981', '93894', '94746', '93816', '93904', '03887', '14752', '93810', '53981', '04953', '14776', '14777', '53983', '54723', '14770', '14771', '94765', '94761', '93986', '14778', '03968', '53952', '53950', '94892', '94891', '54757', '13802', '23293', '54734', '54735', '54733', '04789', '64775', '04787', '64776', '04783', '54738', '54739', '93823', '93822', '14740', '23272', '53802', '54811', '13922', '03985', '14742', '03972', '04868', '14747', '53822', '93227', '93817', '93228', '14748', '04867', '04866', '03879', '23289', '53891', '04862', '53944', '53947', '64705', '53942', '64707', '23203', '53935', '54704', '63853', '94982', '14834', '64761', '04751', '04876', '04871', '04873', '04739', '04879', '93232', '93231', '14758', '13902', '14754', '23240', '94741', '14757', '14750', '94979', '94745', '14753', '53939', '12910', '03868', '14923', '03902', '53933', '54831', '94870', '13904', '14775', '64758', '13911', '04996', '64753', '12911', '63840', '63841', '64757', '64756', '13909', '04903', '23250', '23257', '04807', '23254', '04724', '04808', '04726', '23258', '04720', '03999', '13984', '94908', '13986', '23285', '04889', '14931', '54789', '54788', '64706', '94868', '14733', '94866', '54782', '54781', '03948', '54787', '54786', '54785', '03919', '14719', '54768', '54760', '04925', '93997', '54767', '94721', '94723', '94725', '03969', '53999', '04725', '23244', '94728', '12961', '23907', '14739', '53913', '54792', '54793', '14732', '93211', '04950', '14736', '14737', '14734', '14735', '04899', '53938', '04890', '03928', '04896', '04894', '94854', '53997', '94822', '54778', '54779', '13966', '13967', '04949', '14786', '54770', '94959', '54777', '23239', '94733', '93943', '04947', '14703', '14702', '12979', '04742', '23230', '04741', '14990', '23234', '53986', '12971', '94846', '23237', '03932', '04930', '53909', '53897', '54808', '13919', '14816', '03838', '14819', '03933', '63814', '63817', '63810', '53886', '13958', '14790', '14792', '54756', '14794', '93942', '54790', '04838', '93978', '14712', '13961', '14714', '14715', '14717', '63878', '04831', '94705', '94704', '94702', '54773', '13999', '14880', '93947', '03958', '94789', '93984', '93985', '53977', '53976', '64774', '53979', '93989', '54742', '53150', '23211', '13940', '14937', '13945', '04921', '14842', '53887', '04880', '53889', '54771', '04781', '94794', '94790', '14707', '54780', '14761', '14760', '14763', '14762', '04845', '14768', '54746', '53964', '53965', '54743', '63901', '54740', '13809', '03950', '53969', '03957', '03954', '94737']
    # If you uncomment the below, all daily weather will be added up through 2014.
    #insert_data_in_month(7,1996, 12,2014, no_hourly=True, debug=True)
    insert_data_in_month(12,2014, 12,2014, no_daily=True, debug=True, weather_stations_list = wban_list)
    pass

