IMPORT FROM CSV FILE 'core/demo/sample.csv' INTO sample(usage double, latency double, location string, version string);

SELECT * FROM
  DIFF
    (SELECT * FROM sample WHERE usage > 1000.0) outliers,
    (SELECT * FROM sample WHERE usage < 1000.0) inliers
  ON
    location, version
  COMPARE BY
    risk_ratio(COUNT(*));

SELECT * FROM
  DIFF
    (SELECT * FROM sample WHERE usage > 1000.0) outliers,
    (SELECT * FROM sample WHERE usage < 1000.0) inliers
  ON
    location, version
  COMPARE BY
    risk_ratio(COUNT(*)) WHERE risk_ratio > 1.0;
