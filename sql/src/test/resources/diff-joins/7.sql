IMPORT FROM CSV FILE 'src/test/resources/diff-joins/R7.csv' into R(A1 string);
IMPORT FROM CSV FILE 'src/test/resources/diff-joins/S7.csv' into S(A1 string);
IMPORT FROM CSV FILE 'src/test/resources/diff-joins/T7.csv' into T(A1 string, state string, app string);
SELECT * FROM DIFF (SELECT * FROM R NATURAL JOIN T), (SELECT * FROM S NATURAL JOIN T) ON app, state WITH MIN SUPPORT 0.01 ORDER BY app;
