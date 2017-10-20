# This file contains a list of strings that are valid SQL queries using the DIFF operator
# Each line contains a single SQL query.
SELECT * FROM DIFF t1 ON * COMPARE BY risk_ratio(a);
SELECT * FROM DIFF t1, t2 ON * COMPARE BY risk_ratio(a);
SELECT * FROM DIFF t1, t2 ON a COMPARE BY risk_ratio(b);
SELECT * FROM DIFF t1, t2 ON a COMPARE BY risk_ratio(COUNT(*));
SELECT * FROM DIFF t1, t2 ON a, b COMPARE BY risk_ratio(c);
SELECT * FROM DIFF t1, t2 ON a, b COMPARE BY risk_ratio(c) ORDER BY a;
SELECT * FROM DIFF t1, t2 ON a, b COMPARE BY risk_ratio(c) ORDER BY a DESC;
SELECT * FROM DIFF t1, t2 ON a, b COMPARE BY risk_ratio(c) LIMIT 5;
SELECT * FROM DIFF t1, t2 ON a, b COMPARE BY risk_ratio(c) ORDER BY a DESC LIMIT 1;
SELECT * FROM DIFF t1, t2 ON a, b COMPARE BY risk_ratio(c) MAX COMBO 4;
SELECT * FROM DIFF t1, t2 ON a, b COMPARE BY risk_ratio(c) MAX COMBO 4 ORDER BY a;
SELECT * FROM DIFF t1, t2 ON a, b COMPARE BY risk_ratio(c) MAX COMBO 4 ORDER BY a DESC;
SELECT * FROM DIFF t1, t2 ON a, b COMPARE BY risk_ratio(c) MAX COMBO 4 ORDER BY a DESC LIMIT 1;
SELECT * FROM DIFF t1, t2 ON a, b COMPARE BY risk_ratio(COUNT(*)) MAX COMBO 4 WHERE d = 1 ORDER BY a DESC LIMIT 1;
SELECT * FROM DIFF (SELECT * FROM t1) x, (SELECT * FROM t2) y ON a, b, c COMPARE BY risk_ratio(COUNT(distinct d)) MAX COMBO 2;
