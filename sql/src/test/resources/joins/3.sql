IMPORT FROM CSV FILE 'src/test/resources/joins/3_a.csv' INTO a(A0 string, A1 string, A2 string);
IMPORT FROM CSV FILE 'src/test/resources/joins/3_b.csv' INTO b(A1 string, A2 string, A3 string);
SELECT * FROM a JOIN b USING (A1) ORDER BY A1;
