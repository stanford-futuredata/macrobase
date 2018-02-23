IMPORT FROM CSV FILE 'core/demo/sample.csv' INTO sample(usage double, latency
  double, location string, version string);

SELECT * FROM
  DIFF
    (SELECT * FROM sample WHERE usage > 1000.0) outliers,
    (SELECT * FROM sample WHERE usage < 1000.0) inliers
  ON *;

SELECT * FROM
  DIFF
    (SELECT * FROM sample WHERE usage > 1000.0) outliers,
    (SELECT * FROM sample WHERE usage < 1000.0) inliers
  ON
    location, version;

SELECT * FROM
  DIFF
    (SELECT * FROM sample WHERE usage > 1000.0) outliers,
    (SELECT * FROM sample WHERE usage < 1000.0) inliers
  ON
    location, version
  WITH MIN RATIO 5.0 MIN SUPPORT 0.75
  COMPARE BY
    risk_ratio(COUNT(*));

SELECT * FROM
  DIFF
    (SELECT * FROM sample WHERE usage > 1000.0) outliers,
    (SELECT * FROM sample WHERE usage < 1000.0) inliers
  ON *
  WITH MIN SUPPORT 0.75 MIN RATIO 5.0
  COMPARE BY
    risk_ratio(COUNT(*));

SELECT * FROM
  DIFF
    (SPLIT (
        SELECT *, percentile(usage) as pct FROM sample)
     WHERE pct > 0.9641)
  ON *
  WITH MIN SUPPORT 0.75 MIN RATIO 5.0;
