import pandas as pd
import numpy as np
from sklearn import linear_model, cluster
from collections import defaultdict, Iterable
from itertools import chain, combinations
import operator
import psycopg2

conn = psycopg2.connect("dbname='postgres' user='pbailis' host='localhost'")

cur = conn.cursor()

cols = "hardware_manufacturer,hardware_model,hardware_carrier,android_fw_version,hardware_bootloader,start_reason,stop_reason,hidden_by_support"

ZSCORE = 3

target = "data_count_minutes"
target_score = "1/GREATEST(0.1, data_count_minutes)"
pred = " < 1000"

limit = "LIMIT 100000"

to_select = target+","+cols

SUPPORT = .02

sql = """SELECT %s FROM mapmatch_history H, sf_datasets D, (SELECT avg(%s), stddev(%s) FROM mapmatch_history) x WHERE H.dataset_id = D.id AND %s %s AND @ (%s-x.avg)/x.stddev > %f %s;""" % (to_select, target_score, target_score, target, pred, target_score, ZSCORE, limit)
print sql
cur.execute(sql)

colnames = [desc[0] for desc in cur.description]

cur_score = None
cur_rows = []

df = None

data = pd.DataFrame(cur.fetchall(), columns=colnames)
features = data.drop(target, 1)

print len(data)

pd.set_option('display.max_rows', len(features))
pd.set_option('expand_frame_repr', False)

dummies = pd.get_dummies(features, prefix_sep="//")


print "REGRESSION"


scores = [1./max(r[0], .0001) for r in data.itertuples()]

regr = linear_model.LinearRegression().fit(dummies, scores)

c_with_index = zip(range(0, len(regr.coef_)), regr.coef_)

c_with_index.sort(key = lambda x: x[1])
c_with_index.reverse()

MAX_PRINT = 50

for n in range(0, MAX_PRINT):
    (dim, c) = c_with_index[n]
    print ": ".join(dummies.columns[dim].split("//")), c
    


NCLUSTERS = 10#int(np.sqrt(len(data)/2))

print "K MEANS RESULTS (%d CLUSTERS):" % NCLUSTERS
km = cluster.KMeans(n_clusters=NCLUSTERS).fit(dummies)


THRESH = .5

for centerno in range(0, len(km.cluster_centers_)):
    center = km.cluster_centers_[centerno]
    nmatches = len([i for i in km.labels_ if i == centerno])
    target_vals = [data.iloc[i, 0] for i in range(0, len(data)) if km.labels_[i] == centerno]

    print "\n"
    print "N: %d, Average: %f, Std.: %f" % (nmatches, float(sum(target_vals))/len(target_vals), np.std(target_vals))
    
    for dim in range(0, len(center)):
        val = center[dim]
        if val > THRESH:
            print ": ".join(dummies.columns[dim].split("//")), val

print "\nDBSCAN RESULTS:"
dbscan = cluster.DBSCAN(eps=pow(2, .5)).fit(dummies)
            
for centerno in range(0, len(dbscan.components_)):
    center = dbscan.components_[centerno]
    nmatches = len([i for i in dbscan.labels_ if i == centerno])
    target_vals = [data.iloc[i, 0] for i in range(0, len(data)) if dbscan.labels_[i] == centerno]

    if nmatches == 0:
        continue
    
    print "\n"
    print "N: %d, Average: %f, Std.: %f" % (nmatches, float(sum(target_vals))/len(target_vals), np.std(target_vals))
    
    for dim in range(0, len(center)):
        val = center[dim]
        if val > THRESH:
            print ": ".join(dummies.columns[dim].split("//")), val

'''
birch = cluster.Birch(threshold=1).fit(dummies)
print len(birch.subcluster_centers_)
print
for center in birch.subcluster_centers_:
    
    for dim in range(0, len(center)):
        val = center[dim]
        if val > THRESH:
            print dim, val, dummies.columns[dim].split("//"),
                                                    

'''
