SELECT * FROM
  DIFF
    (SPLIT sample WHERE usage > 1000.0)
  ON * INTO OUTFILE '3.csv';
