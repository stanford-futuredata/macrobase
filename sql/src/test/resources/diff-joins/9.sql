IMPORT FROM CSV FILE 'src/test/resources/diff-joins/R9.csv' into R(A0 string);
IMPORT FROM CSV FILE 'src/test/resources/diff-joins/S9.csv' into S(A0 string);
IMPORT FROM CSV FILE 'src/test/resources/diff-joins/T9.csv' into T(A0 string, A1 string, A2 string, A3 string, A4 string);
SELECT * FROM DIFF (SELECT * FROM R NATURAL JOIN T), (SELECT * FROM S NATURAL JOIN T) ON A1, A2, A3, A4 WITH MIN SUPPORT 0.01 ORDER BY A1;
