# Plenar.io Weather README

Plenar.io uses the Quality Controlled Local Climatological Data (QCLCD) weather data from the National Oceanic and Atmospheric Administration (NOAA).

See [http://cdo.ncdc.noaa.gov/qclcd/qclcddocumentation.pdf](QCLCD Documentation) for details.

## Daily Observations

* `wban_code`: The WBAN (Weather Bureau Army Navy) number is the unique identifier for weather stations.
* `date`: in SQL date form (e.g. '2003-10-05').

* `temp_max`: the maximum temperature for the day in Fahrenheit.

* `temp_min`: the minimum temperature for the day in Fahrenheit.

* `temp_avg`: the average temperature for the day in Fahrenheit.

* `departure_from_normal`: the departure from the normal average temperature for the day in Fahrenheit.

* `dewpoint_avg`: the temperature at which water vapor condenses into water at the same rate at which it evaporates, in Fahrenheit.

* `wetbulb_avg`: the temperature the air _would_ have at 100% humidity, in Fahrenheit. (One can conceivably use this value to help predict freezing rain events.)

* `weather_types`: this is a multidimensional PostgreSQL array which contains values from  [FEDERAL METEOROLOGICAL HANDBOOK No. 1: Surface Weather Observations and Reports](http://www.ofcm.gov/fmh-1/fmh1.htm), in the following six-tuple: `[vicinity, intensity, desc, precip, obscuration, other]`

   So for example, in the event of an observation of a thunderstorm with heavy rain and snow with fog, one might see:
`[NULL, +, TS, RA, DG, NULL]`

   But if it were also snowing, there would be an additional six-tuple: `[NULL, NULL, NULL, SN, NULL, NULL]`

* `snowice_depth`: In inches, the current depth of snow/ice. A 'trace' (**T**) is encoded as 0.005.

* `snowice_waterequiv`: This would be the depth of water in inches if you could melt the snowpack instantaneously. 

* `snowfall`: Snowfall in inches on this day.

* `precip_total`: Total precipitation in inches, during the 24-hour period ending in local standard time.

* `station_pressure`: average pressure in "Hg (inches of Mercury).

* `sealevel_pressure`: average sea-level pressure in "Hg (inches of Mercury).

* `resultant_windspeed`, `resultant_winddirection`: The magnitude (in miles per hour) and direction (in degrees) of `resultant wind`, which is obtained by converting recorded wind speeds and directions over a 24-hour period into a single vector with a single magnitude and direction.

* `resultant_winddirection_cardinal`: the above wind direction converted to human-readable direction, e.g. N, NE, NNE, NNW.

* `avg_windspeed`: The average wind speed over the 24-hour period.

* `max5_windspeed`, `max5_winddirection`, `max5_direction_cardinal`: The maximum wind speed and direction of recorded 5-*second* averages.

* `max2_windspeed`, `max2_winddirection`, `max2_direction_cardinal`: The maximum wind speed and direction of recorded 2-*minute* averages. (This is considered "sustained" wind.)


## Hourly Observations

* `wban_code`: The WBAN (Weather Bureau Army Navy) number is the unique identifier for weather stations.

* `datetime`:  in SQL datetime form (e.g. '2003-10-05 23:04:00').

* `old_station_type`: (Valid for all dates before May 1st, 2007)
  * AO1: automated station without a precipitation discriminator (no rain/snow sensor).
  * AO2; automated station with precipitation discriminator.

* `station_type`: (Valid for all dates after May 1st, 2007)
  * 0 AMOS now AWOS, also USAF stations
  * 4 MAPSO
  * 5 Navy METAR
  * 6 Navy Airways(obsolete)
  * 8 SOD- Keyed from 10C
  * 9 SOD/HPD- Keyed B16, F-6, Navy Forms
  * 11 ASOS (NWS)
  * 12 ASOS (FAA)
  * 15 Climate Reference Network (CRN)

* `sky_condition`: Up to three strings representing up to three layers of cloud cover. Each string is one of the below abbreviations with (in the case of clouds) a three-digit height following, representing hundreds of feet.

  * CLR: Clear below 12,000 feet
  * FEW: > 0/8 - 2/8 sky cover
  * SCT (SCATTERED):  3/8 - 4/8 sky cover
  * BKN (BROKEN): 5/8 - 7/8 sky cover
  * OVC (OVERCAST): 8/8 sky cover

* `sky_condition_top`: [This parameter will be removed and replaced]

* `visibility`: distance at which objects can be discerned, in statute miles.

* `weather_types`: this is a multidimensional PostgreSQL array which contains values from  [FEDERAL METEOROLOGICAL HANDBOOK No. 1: Surface Weather Observations and Reports](http://www.ofcm.gov/fmh-1/fmh1.htm), in the following six-tuple: `[vicinity, intensity, desc, precip, obscuration, other]`

   So for example, in the event of an observation of a thunderstorm with heavy rain and snow with fog, one might see:
`[NULL, +, TS, RA, DG, NULL]`

   But if it were also snowing, there would be an additional six-tuple: `[NULL, NULL, NULL, SN, NULL, NULL]`


* `drybulb_fahrenheit`: current temperature at current level of humidity, in Fahrenheit.

* `wetbulb_fahrenheit`: the temperature the air _would_ have at 100% humidity, in Fahrenheit. (One can conceivably use this value to help predict freezing rain events.)

* `dewpoint_fahrenheit`: the temperature at which water vapor condenses into water at the same rate at which it evaporates, in Fahrenheit.

* `relative_humidity`: expressed as a percentage (0 to 100), the ratio of the partial pressure of water vapor to the saturated vapor pressure at the current temperature.

* `wind_speed`: observed wind speed (averaged?) in miles per hour.

* `wind_direction`: observed wind direction (averaged?) in degrees (0 to 360).

* `wind_direction_cardinal`: observed wind direction (averaged?) as a human-readable direction, e.g. N, NE, NNE, NNW.

* `station_pressure`: average pressure in "Hg (inches of Mercury).

* `sealevel_pressure`: average sea-level pressure in "Hg (inches of Mercury).

* `report_type`:
  * AA: METAR (AVIATION ROUTINE WEATHER REPORT) – HOURLY
  * SP: METAR SPECIAL REPORT
  * CRN05

* `hourly_precip`: precipitation total in inches.