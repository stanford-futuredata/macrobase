import pandas as pd
import numpy as np
from sklearn import linear_model
from collections import defaultdict, Iterable
from itertools import chain, combinations
import operator
import psycopg2

conn = psycopg2.connect("dbname='postgres' user='pbailis' host='localhost'")

cur = conn.cursor()

cols = "hardware_manufacturer,hardware_model,hardware_carrier,android_fw_version,hardware_bootloader"

sql = """
CREATE TEMP VIEW m AS SELECT data_count_minutes, %s FROM mapmatch_history H, sf_datasets D WHERE H.dataset_id = D.id AND data_count_minutes < 4 LIMIT 10000;

SELECT SUM(1.0/log(data_count_minutes))*pow(COUNT(*), -.5) as score, SUM(1.0/data_count_minutes) as sum, median(data_count_minutes) as median_time, percentile_cont(.01) WITHIN GROUP(ORDER BY data_count_minutes) as p99, percentile_cont(.25) WITHIN GROUP(ORDER BY data_count_minutes) as p75, COUNT(*) as cnt, %s FROM m GROUP BY CUBE(%s) ORDER BY score DESC;""" % (cols, cols, cols)
print sql
cur.execute(sql)

colnames = [desc[0] for desc in cur.description]

cur_score = None
cur_rows = []

df = None

for row in cur.fetchall():
    if cur_score is not None and round(row[0], 8) != round(cur_score, 8):
        if cur_score:
            max_idx = -1
            min_idx_count = 10000000
            for e_no in range(0, len(cur_rows)):
                entry = cur_rows[e_no]
                entry_count = 0
                for val in entry:
                    if val == None:
                        entry_count += 1

                if entry_count < min_idx_count:
                    min_idx_count = entry_count
                    max_idx = e_no


            fd = [f for f in cur_rows[max_idx]]
            fd[1] = float(fd[1])
            data = pd.DataFrame([fd], columns=colnames)
            if df is None:
                df = data
            else:
                df = pd.concat([df, data])
            
        cur_rows = []

    cur_score = row[0]
    cur_rows.append(row)

pd.set_option('display.max_rows', len(df))
pd.set_option('expand_frame_repr', False)
print df
            
                                                    
