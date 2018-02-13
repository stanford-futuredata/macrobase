SELECT usage, percentile(usage) as pct FROM sample INTO OUTFILE '15.csv';
