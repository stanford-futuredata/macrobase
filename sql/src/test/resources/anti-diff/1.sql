IMPORT FROM CSV FILE '../core/demo/sample.csv' into sample(usage double, latency double, location string, version string);
SELECT * FROM ANTI DIFF (SELECT * FROM sample WHERE usage > 1000.0) outliers, (SELECT * FROM sample WHERE usage < 1000.0) inliers ON location, version INTO OUTFILE '1.csv';
