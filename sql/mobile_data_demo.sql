IMPORT FROM CSV FILE 'core/demo/mobile_data.csv' INTO mobile_data(record_id
  string, user_id string, state string, hw_make string, hw_model string,
  firmware_version string, app_version string, avg_temp double, battery_drain
  double, trip_time double);

SELECT battery_drain FROM mobile_data;

SELECT battery_drain FROM mobile_data ORDER BY battery_drain;

SELECT battery_drain FROM mobile_data WHERE battery_drain > 0.90;

SELECT battery_drain FROM mobile_data WHERE battery_drain <= 0.90;

SELECT * FROM DIFF
  (SELECT * FROM mobile_data WHERE battery_drain > 0.90) outliers,
  (SELECT * FROM mobile_data WHERE battery_drain <= 0.90) inliers
  ON state, hw_make, hw_model, firmware_version, app_version;

SELECT app_version, hw_make, hw_model, global_ratio, support, outlier_count
FROM DIFF
  (SELECT * FROM mobile_data WHERE battery_drain > 0.90) outliers,
  (SELECT * FROM mobile_data WHERE battery_drain <= 0.90) inliers
  ON state, hw_make, hw_model, firmware_version, app_version
  ORDER BY global_ratio;

SELECT app_version, hw_make, hw_model, global_ratio, support, outlier_count
FROM DIFF
  (SELECT * FROM mobile_data WHERE battery_drain > 0.90) outliers,
  (SELECT * FROM mobile_data WHERE battery_drain <= 0.90) inliers
  ON state, hw_make, hw_model, firmware_version, app_version
  WITH MIN RATIO 10.0 MIN SUPPORT 0.5
  ORDER BY global_ratio;

SELECT hw_make, battery_drain FROM mobile_data WHERE battery_drain > 0.90 AND hw_make = 'Emdoor';

SELECT hw_make, battery_drain FROM mobile_data WHERE battery_drain <= 0.90 AND hw_make = 'Emdoor';

SELECT app_version, hw_make, hw_model, firmware_version, global_ratio, support, outlier_count
FROM DIFF
  (SELECT * FROM mobile_data WHERE battery_drain > 0.90) outliers,
  (SELECT * FROM mobile_data WHERE battery_drain <= 0.90) inliers
  ON state, hw_make, hw_model, firmware_version, app_version
  MAX COMBO 1
  ORDER BY global_ratio;

SELECT app_version, hw_make, hw_model, firmware_version, global_ratio, support, outlier_count
FROM DIFF
  (SELECT * FROM mobile_data WHERE battery_drain > 0.90) outliers,
  (SELECT * FROM mobile_data WHERE battery_drain <= 0.90) inliers
  ON state, hw_make, hw_model, firmware_version, app_version
  MAX COMBO 2
  ORDER BY global_ratio;

SELECT app_version, hw_make, hw_model, firmware_version, global_ratio, support, outlier_count
FROM DIFF
  (SELECT * FROM mobile_data WHERE battery_drain > 0.90) outliers,
  (SELECT * FROM mobile_data WHERE battery_drain <= 0.90) inliers
  ON state, hw_make, hw_model, firmware_version, app_version
  MAX COMBO 2
  ORDER BY global_ratio;

SELECT app_version, hw_make, hw_model, firmware_version, global_ratio
FROM DIFF
  (SELECT * FROM mobile_data WHERE battery_drain > 0.90) outliers,
  (SELECT * FROM mobile_data WHERE battery_drain <= 0.90) inliers
  ON state, hw_make, hw_model, firmware_version, app_version
  ORDER BY global_ratio
  INTO OUTFILE 'mobile_data_outliers.csv'
  FIELDS TERMINATED BY '\t';

-- single-table queries
SELECT app_version, hw_make, hw_model, global_ratio
FROM DIFF
  (SPLIT ON PERCENTILE(battery_drain, 0.9) FROM mobile_data)
  ON state, hw_make, hw_model, app_version
  ORDER BY global_ratio;

-- This gives an OOM error, since includeHi and includeLo are both false
--SELECT app_version, hw_make, hw_model, global_ratio
--FROM DIFF
--  (SPLIT ON PERCENTILE(battery_drain, 0.9, false) FROM mobile_data)
--  ON state, hw_make, hw_model, app_version
--  ORDER BY global_ratio;

SELECT app_version, hw_make, hw_model, global_ratio
FROM DIFF
  (SPLIT ON PERCENTILE(battery_drain, 0.9, false, true) FROM mobile_data)
  ON state, hw_make, hw_model, app_version
  ORDER BY global_ratio;

SELECT app_version, hw_make, hw_model, global_ratio
FROM DIFF
  (SPLIT ON PERCENTILE(battery_drain, 0.9, true, true) FROM mobile_data)
  ON state, hw_make, hw_model, app_version
  ORDER BY global_ratio;

-- Should be an error
SELECT app_version, hw_make, hw_model, global_ratio
FROM DIFF
  (SPLIT ON PERCENTILE(battery_drain) FROM mobile_data)
  ON state, hw_make, hw_model, app_version
  ORDER BY global_ratio;

-- Should also be an error
SELECT app_version, hw_make, hw_model, global_ratio
FROM DIFF
  (SPLIT ON PERCENTILE(0.9) FROM mobile_data)
  ON state, hw_make, hw_model, app_version
  ORDER BY global_ratio;


-- Should be same as the original double-table query above
SELECT app_version, hw_make, hw_model, global_ratio
FROM DIFF
  (SPLIT ON PREDICATE(battery_drain, ">", 0.9) FROM mobile_data)
  ON state, hw_make, hw_model, app_version
  ORDER BY global_ratio;

-- Should yield no results
SELECT app_version, hw_make, hw_model, global_ratio
FROM DIFF
  (SPLIT ON PREDICATE(battery_drain, "!=", 0.9) FROM mobile_data)
  ON state, hw_make, hw_model, app_version
  ORDER BY global_ratio;

-- Should be an error
SELECT app_version, hw_make, hw_model, global_ratio
FROM DIFF
  (SPLIT ON PREDICATE(battery_drain, ">") FROM mobile_data)
  ON state, hw_make, hw_model, app_version
  ORDER BY global_ratio;

-- Should also be an error
SELECT app_version, hw_make, hw_model, global_ratio
FROM DIFF
  (SPLIT ON PREDICATE(battery_drain, 0.9) FROM mobile_data)
  ON state, hw_make, hw_model, app_version
  ORDER BY global_ratio;

-- Should also be an error
SELECT app_version, hw_make, hw_model, global_ratio
FROM DIFF
  (SPLIT ON PREDICATE(battery_drain) FROM mobile_data)
  ON state, hw_make, hw_model, app_version
  ORDER BY global_ratio;

