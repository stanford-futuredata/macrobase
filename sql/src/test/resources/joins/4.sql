IMPORT FROM CSV FILE 'src/test/resources/joins/4_a.csv' INTO a(A0 string, A1 string);
IMPORT FROM CSV FILE 'src/test/resources/joins/4_b.csv' INTO b(A1 string, A2 string);
SELECT * FROM a NATURAL JOIN b ORDER BY A1;
