IMPORT FROM CSV FILE '../core/demo/sample.csv' into sample(usage double, latency double, location string, version string);
SELECT * FROM ANTI DIFF (SPLIT sample WHERE usage > 1000.0) ON location, version WITH MIN RATIO 10.0 MIN SUPPORT 0.05 MAX COMBO 1 INTO OUTFILE '2.csv';
