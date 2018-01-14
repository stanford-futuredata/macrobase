IMPORT FROM CSV FILE 'core/demo/sample.csv' INTO sample(usage double, latency double, location string, version string);

IMPORT FROM CSV FILE 'core/demo/mobile_data.csv' INTO mobile_data(record_id
  string, user_id string, state string, hw_make string, hw_model string,
  firmware_version string, app_version string, avg_temp double, battery_drain
  double, trip_time double);

