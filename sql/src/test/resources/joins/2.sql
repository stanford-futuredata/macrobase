IMPORT FROM CSV FILE 'src/test/resources/joins/2_a.csv' INTO a(A0 string, A1 string);
IMPORT FROM CSV FILE 'src/test/resources/joins/2_b.csv' INTO b(A1 string, A2 string);
SELECT * FROM a JOIN b USING (A1) ORDER BY A1;
