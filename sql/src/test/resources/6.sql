SELECT * FROM
  DIFF
    (SPLIT sample WHERE usage > 1000.0)
  ON
    location, version
  COMPARE BY
    risk_ratio(COUNT(*)) INTO OUTFILE '6.csv';
