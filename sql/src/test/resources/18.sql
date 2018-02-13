SELECT *, percentile(usage) as pct FROM sample WHERE pct > 0.95 INTO OUTFILE '18.csv';
