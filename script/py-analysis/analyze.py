import pandas as pd
import numpy as np
from sklearn import linear_model
from collections import defaultdict, Iterable
from itertools import chain, combinations
import operator

# 0 == best match, any columns; higher -> fewer predicates
FEW_PREDICATE_PRIORITY = -1

# 0 == average; higher -> larger groups
MANY_RESULTS_PRIORITY = .1

# 1 = take the average, 0 == ignore scores
SCORE_PRIORITY = 0

 
MAX_RESULTS = 10
DO_FILTER = True

QUERY_LIMIT = 100000

import psycopg2

def lower_worse(r):
    return 1./max(r, 0.000001)

def lower_better(r):
    return r

def powerset(iterable):
    s = list(iterable)
    return chain.from_iterable(combinations(s, r) for r in (range(1,len(s)+1)))

examine_cols = ["hardware_manufacturer", "hardware_model", "hardware_carrier", "android_fw_version", "android_api_version"]

targets = [("data_count_accel_samples", "< 100000 ", lower_worse),
           ("data_count_minutes", "< 10", lower_worse),
           ("battery_total_drain", "> 10", lower_better)]

for target in targets:
    target_col = target[0]
    target_val = target[1]

    score_fn = target[2]

    all_cols = examine_cols + [target_col]

    conn = psycopg2.connect("dbname='postgres' user='pbailis' host='localhost'")

    cur = conn.cursor()

    sql = "SELECT D.id, %s FROM mapmatch_history H, sf_datasets D WHERE H.dataset_id = D.id AND %s %s %s" % (",".join(all_cols), target_col, target_val, "LIMIT "+str(QUERY_LIMIT) if QUERY_LIMIT else "")
    print sql
    cur.execute(sql)

    colnames = [desc[0] for desc in cur.description]


    data = pd.DataFrame(cur.fetchall(), columns = colnames)

    # calculate scores
    data[target_col] = data[target_col].apply(score_fn)

    # normalize scores
    max_score = data[target_col].max()
    data[target_col] = data[target_col].div(max_score)

    groups_to_scores = {}
    groups_to_counts = {}
    groups_to_sums = {}
    groups_to_df = {}

    total_sum = data[target_col].sum()
    total_rows = data.shape[0]

    for grouping in powerset(examine_cols):
        for name, group in data.groupby(by=grouping):
            sum_scores = group[target_col].sum()
            num_entries = group.shape[0]
            score = pow(sum_scores/total_sum/num_entries, SCORE_PRIORITY)*pow(len(grouping)/float(len(examine_cols)), -FEW_PREDICATE_PRIORITY)*pow(num_entries/float(total_rows), MANY_RESULTS_PRIORITY)

            # are you kidding me
            if len(grouping) == 1:
                name = (name,)
            c_name = tuple(zip(grouping, name))

            groups_to_df[c_name] = group
            groups_to_scores[c_name] = score
            groups_to_sums[c_name] = sum_scores
            groups_to_counts[c_name] = group.size

    print "RESULTS FOR %s %s (N=%d):\n" % (target_col, target_val, data.shape[0])

    revsorted = sorted(groups_to_scores.iteritems(), key=operator.itemgetter(1), reverse=True)
    
    output = []
    tuplenum = 0
    while True:
        # if we've exhausted all combos
        if tuplenum == len(revsorted):
            break
        
        i = revsorted[tuplenum]
        tuplenum += 1
        g, s = i[0], i[1]

        group_df = groups_to_df[g]

        # filter out subsets of stuff we've already seen
        subset_seen = False
        if DO_FILTER:
            for o in output:
                if pd.merge(group_df, o, how='inner', on=['id']).size > 0:
                    subset_seen = True
                    break
                '''
                os = set(o)
                gs = set(g)
                if os.intersection(gs):
                    subset_seen = True
                    break
                '''                                    

        if subset_seen:
            continue
        
        prettygroup = "(%s)" % ("; ".join("%s: %s" % (d[0], d[1]) for d in g))
        print prettygroup, s, groups_to_sums[g]/groups_to_counts[g], groups_to_counts[g]


        output.append(group_df)
        if len(output) == MAX_RESULTS:
            break

    print

