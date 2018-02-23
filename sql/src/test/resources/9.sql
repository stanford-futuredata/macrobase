SELECT * FROM
  DIFF
    (SPLIT sample WHERE usage > 1000.0)
  ON
    location, version
  COMPARE BY
    risk_ratio(COUNT(*))
  MAX COMBO 1 INTO OUTFILE '9.csv';
