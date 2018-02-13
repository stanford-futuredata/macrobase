SELECT usage, percentile(usage) as pct FROM sample WHERE pct > 0.95 INTO OUTFILE '19.csv';
