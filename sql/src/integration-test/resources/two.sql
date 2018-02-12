SELECT * FROM
  DIFF
    (SELECT * FROM sample WHERE usage > 1000.0) outliers,
    (SELECT * FROM sample WHERE usage < 1000.0) inliers
  ON
    location, version
  WITH MIN RATIO 5.0 MIN SUPPORT 0.75
  COMPARE BY
    risk_ratio(COUNT(*));
