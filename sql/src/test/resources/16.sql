SELECT *, percentile(usage) as pct FROM sample INTO OUTFILE '16.csv';
