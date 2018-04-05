IMPORT FROM CSV FILE 'src/test/resources/joins/1_a.csv' INTO a(A0 string);
IMPORT FROM CSV FILE 'src/test/resources/joins/1_b.csv' INTO b(A0 string);
SELECT * FROM a JOIN b ON A0 ORDER BY A0;
