IMPORT FROM CSV FILE 'src/test/resources/anti-diff/sample.csv' into sample(usage double, latency double, location string, version string, device string);
SELECT * FROM ANTI DIFF (SPLIT sample WHERE usage > 1000.0) on location, version, device WITH MIN RATIO 15.0 MIN SUPPORT 0.05;
