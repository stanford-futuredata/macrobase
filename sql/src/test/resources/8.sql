SELECT * FROM
  DIFF
    (SPLIT sample WHERE usage > 1000.0)
  ON
    location, version
  WITH MIN RATIO 10.0 MIN SUPPORT 0.05
  COMPARE BY
    risk_ratio(COUNT(*)) INTO OUTFILE '8.csv';
