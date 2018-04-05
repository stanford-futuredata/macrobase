IMPORT FROM CSV FILE 'src/test/resources/diff-joins/R11.csv' into R(A0 string);
IMPORT FROM CSV FILE 'src/test/resources/diff-joins/S11.csv' into S(A0 string);
IMPORT FROM CSV FILE 'src/test/resources/diff-joins/T11.csv' into T(A0 string, A1 string, A2 string, A3 string);
SELECT * FROM DIFF (SELECT * FROM R NATURAL JOIN T), (SELECT * FROM S NATURAL JOIN T) ON A1, A2, A3 WITH MIN SUPPORT 0.01 MIN RATIO 1.5 ORDER BY global_ratio DESC;
