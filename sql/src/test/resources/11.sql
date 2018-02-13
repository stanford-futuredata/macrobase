SELECT * FROM
  DIFF
    (SPLIT sample WHERE usage > 1000.0)
  ON
    location, version
  COMPARE BY
    risk_ratio(COUNT(*))
  ORDER BY support INTO OUTFILE '11.csv';
