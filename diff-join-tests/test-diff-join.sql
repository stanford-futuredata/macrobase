IMPORT FROM CSV FILE 'diff-join-tests-advanced/R1.csv' into R(A1 string);
IMPORT FROM CSV FILE 'diff-join-tests-advanced/S1.csv' into S(A1 string);
IMPORT FROM CSV FILE 'diff-join-tests-advanced/T1.csv' into T(A1 string, state string, app string);
SELECT * FROM R NATURAL JOIN T;
SELECT * FROM R JOIN T USING (A1);
SELECT * FROM S NATURAL JOIN T;
SELECT * FROM S JOIN T USING (A1);
SELECT * FROM DIFF (SELECT * FROM R JOIN T USING (A1)), (SELECT * FROM S JOIN T USING (A1)) ON state, app WITH MIN SUPPORT 0.01;
SELECT * FROM DIFF (SELECT * FROM R NATURAL JOIN T), (SELECT * FROM S NATURAL JOIN T) ON app, state WITH MIN SUPPORT 0.01;
