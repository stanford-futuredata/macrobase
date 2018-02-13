SELECT * FROM
  DIFF
    (SPLIT sample WHERE usage > 1000.0)
  ON
    location, version
  WITH MIN RATIO 10.0 MIN SUPPORT 0.05
  COMPARE BY
    risk_ratio(COUNT(*))
  MAX COMBO 1 INTO OUTFILE '10.csv';
