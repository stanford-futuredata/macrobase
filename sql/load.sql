IMPORT FROM CSV FILE 'core/demo/sample.csv' INTO sample(usage double, latency
  double, location string, version string);

IMPORT FROM CSV FILE 'core/demo/mobile_data.csv' INTO mobile_data(record_id
  string, user_id string, state string, hw_make string, hw_model string,
  firmware_version string, app_version string, avg_temp double, battery_drain
  double, trip_time double);

IMPORT FROM CSV FILE 'core/demo/wikiticker.csv' INTO wiki(time string, channel
  string, cityName string, comment string, countryIsoCode string, countryName
  string, isAnonymous string, isMinor string, isNew string, isRobot string,
  isUnpatrolled string, metroCode string, namespace string, page string,
  regionIsoCode string, regionName string, user string, delta double, added
  double, deleted double);

