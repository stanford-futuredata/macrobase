SELECT * FROM
  DIFF
    (SPLIT sample WHERE usage > 1000.0)
  ON location, version INTO OUTFILE '2.csv';
