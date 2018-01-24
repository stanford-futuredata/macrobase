IMPORT FROM CSV FILE 'core/demo/sample.csv' INTO sample(usage double, latency
  double, location string, version string);

IMPORT FROM CSV FILE 'core/demo/mobile_data.csv' INTO mobile_data(record_id
 string, user_id string, state string, hw_make string, hw_model string,
  firmware_version string, app_version string, avg_temp double, battery_drain
  double, trip_time double);

IMPORT FROM CSV FILE 'wikiticker.csv' INTO wiki(time string, user string, page
  string, channel string, namespace string, comment string, metroCode string,
  cityName string, regionName string, regionIsoCode string, countryName string,
  countryIsoCode string, isAnonymous string, isMinor string, isNew string,
  isRobot string, isUnpatrolled string, delta double, added double, deleted
  double);
