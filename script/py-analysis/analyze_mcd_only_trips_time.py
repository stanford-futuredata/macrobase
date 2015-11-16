import pandas as pd
import numpy as np
import scipy
from sklearn import linear_model, cluster, covariance
from collections import defaultdict, Iterable
from itertools import chain, combinations
import operator
import psycopg2
import sys
import json

conn = psycopg2.connect("dbname='postgres' user='pbailis' host='localhost'")

cur = conn.cursor()

cols = "hardware_manufacturer,hardware_model,hardware_carrier,android_fw_version,hardware_bootloader,start_reason,stop_reason,hidden_by_support"

ZSCORE = 3

limit = "LIMIT 100000"

SUPPORT = .02

N_DEPENDENT = 6

sql = """SELECT COUNT(*) as trip_cnt, avg(data_count_minutes) AS avg_minutes,  avg(score_di_brake) AS score_brake, avg(distance_pct_path_error) as distance_pct_path_error, avg(classification_confidence) as classification_confidence, avg(suspension_fit_error) as suspension_fit_error, userid  FROM mapmatch_history H, sf_datasets D WHERE H.dataset_id = D.id GROUP BY userid  %s;""" % (limit)

print sql
cur.execute(sql)

colnames = [desc[0] for desc in cur.description]

raw_data = cur.fetchall()

df = pd.DataFrame(raw_data, columns=colnames)
dependents = df.drop('userid', 1)
dependents = dependents.drop('distance_pct_path_error', 1).drop('classification_confidence', 1).drop('suspension_fit_error', 1).fillna(0)
dependents = dependents.apply(lambda x: (x - np.min(x)) / (np.max(x) - np.min(x)))

mcd = covariance.MinCovDet()
mcd.fit(dependents)
distances = mcd.mahalanobis(dependents-mcd.location_) ** (.5)
distances_with_idx = zip(range(0, len(distances)), distances)
#pctile_cutoff = np.percentile(distances_with_idx, 90)

pctile_cutoff = 0
filtered_distances = [i for i in distances_with_idx if i[1] > pctile_cutoff]
filtered_distances.sort(key = lambda x: -x[1])
filtered_distances = filtered_distances[:21]


max_trips = max(df['trip_cnt'])
trip_center = mcd.location_[0]*max_trips

max_minutes = max(df['avg_minutes'])
minutes_center = mcd.location_[1]*max_minutes


print "DEVIATIONS AT 95th PERCENTILE"
print

for (idx, dist) in filtered_distances:
    e = [i if i else 0 for i in raw_data[idx]]
    print "User %s ss_dist: %.02f" % (e[-1], dist)
    print "Total trip count: %d (c_delta: %.02f; %.02f%%)" % (e[0], e[0]-trip_center, (e[0]-trip_center)*100/trip_center)
    print "Average minutes: %.02f (c_delta: %.02f, %.02f%%)" % (e[1], e[1] - minutes_center, (e[1] - minutes_center)*100/minutes_center)

    print



