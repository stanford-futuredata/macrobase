IMPORT FROM CSV FILE '../core/demo/sample.csv' INTO sample;
SELECT * FROM DIFF (SELECT * FROM sample WHERE usage > 1000.0) outliers, (SELECT * FROM sample WHERE usage < 1000.0) inliers ON location, version COMPARE BY pmi_ratio(COUNT(*)) MAX COMBO 2;
SELECT * FROM DIFF (SELECT * FROM sample WHERE usage > 1000.0) outliers, (SELECT * FROM sample WHERE usage < 1000.0) inliers ON location, version COMPARE BY pmi_ratio(COUNT(*)) MAX COMBO 2 WHERE prevalence_ratio > 10.0;
