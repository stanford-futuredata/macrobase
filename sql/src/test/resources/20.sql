SELECT usage FROM sample WHERE percentile(usage) > 0.95 INTO OUTFILE '20.csv';
