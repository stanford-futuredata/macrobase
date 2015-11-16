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
dependents = dependents.fillna(0)
dependents = dependents.apply(lambda x: (x - np.min(x)) / (np.max(x) - np.min(x)))

mcd = covariance.MinCovDet()
mcd.fit(dependents)
distances = mcd.mahalanobis(dependents-mcd.location_) ** (.5)
distances_with_idx = zip(range(0, len(distances)), distances)
pctile_cutoff = np.percentile(distances_with_idx, 95)

filtered_distances = [i for i in distances_with_idx if i[1] > pctile_cutoff]
filtered_distances.sort(key = lambda x: -x[1])


max_trips = max(df['trip_cnt'])
trip_center = mcd.location_[0]*max_trips

max_minutes = max(df['avg_minutes'])
minutes_center = mcd.location_[1]*max_minutes

max_score_brake = max(df['score_brake'])
brake_center = mcd.location_[2]*max_score_brake

max_path_error = max(df['distance_pct_path_error'])
path_center = mcd.location_[3]*max_path_error

max_classification_confidence = max(df['classification_confidence'])
classification_confidence_center = mcd.location_[4]

max_suspension_fit_error = max(df['suspension_fit_error'])
suspension_center = mcd.location_[5]*max_suspension_fit_error


print "DEVIATIONS AT 95th PERCENTILE"
print

for (idx, dist) in filtered_distances:
    e = [i if i else 0 for i in raw_data[idx]]
    print "User %s ss_dist: %.02f" % (e[-1], dist)
    print "Total trip count: %d (c_delta: %.02f; %.02f%%)" % (e[0], e[0]-trip_center, (e[0]-trip_center)*100/trip_center)
    print "Average minutes: %.02f (c_delta: %.02f, %.02f%%)" % (e[1], e[1] - minutes_center, (e[1] - minutes_center)*100/minutes_center)

    print "Average brake error: %.02f (c_delta: %.02f, %.02f%%)" % (e[2], e[2] - brake_center, (e[2] - brake_center)*100/brake_center)
    print "Average path error: %.02f (c_delta: %.02f, %.02f%%)" % (e[3], e[3]-path_center, (e[3]-path_center)*100/path_center)

    cc = e[4]
    if not cc:
        cc = 1
    print "Average classification confidence: %s (c_delta: %.02f, %.02f%%)" % (cc, cc-classification_confidence_center, (cc-classification_confidence_center)*100/classification_confidence_center)

    sf = e[5]
    if not sf:
        sf = 1
    
    print "Average suspension fit error: %.02f (c_delta: %.02f, %.02f%%)" % (sf, sf-suspension_center, (sf-suspension_center)*100/suspension_center)

    print



