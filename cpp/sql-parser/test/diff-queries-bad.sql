# This file contains a list of strings that are NOT valid SQL queries.
# Each line contains a single SQL query.
# Each line starts with a '!' char to indicate that parsing should fail.
!SELECT * FROM DIFF t1, t2 ON *;
!SELECT * FROM DIFF t1, t2, t3 ON * COMPARE BY a;
!SELECT * FROM DIFF t1, t2 ON a, b COMPARE BY c ORDER BY a MAX COMBO 4;
!SELECT * FROM DIFF t1, t2 ON * COMPARE BY a;
