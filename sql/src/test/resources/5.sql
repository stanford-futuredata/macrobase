SELECT * FROM
  DIFF
    (SPLIT sample WHERE usage > 1000.0)
  ON location, version
  MAX COMBO 1 INTO OUTFILE '5.csv';
