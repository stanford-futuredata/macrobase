IMPORT FROM CSV FILE 'core/demo/mobile_data.csv' INTO mobile_data(record_id
  string, user_id string, state string, hw_make string, hw_model string,
  firmware_version string, app_version string, avg_temp double, battery_drain
  double, trip_time double);

SELECT * FROM DIFF
  (SELECT * FROM mobile_data WHERE battery_drain > 0.45) outliers,
  (SELECT * FROM mobile_data WHERE battery_drain < 0.45) inliers
  ON state, hw_make, hw_model, firmware_version, app_version
  COMPARE BY risk_ratio(COUNT(*));


