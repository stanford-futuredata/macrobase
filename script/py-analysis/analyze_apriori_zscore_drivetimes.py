import pandas as pd
import numpy as np
import scipy
from sklearn import linear_model, cluster
from collections import defaultdict, Iterable
from itertools import chain, combinations
import operator
import psycopg2
import sys
import json

conn = psycopg2.connect("dbname='postgres' user='pbailis' host='localhost'")

cur = conn.cursor()

cols = "hardware_manufacturer,hardware_model,hardware_carrier,android_fw_version,hardware_bootloader,start_reason,stop_reason,hidden_by_support,userid"

ZSCORE = 3

target = "data_count_minutes"
target_score = "1/GREATEST(0.1, data_count_minutes)"
pred = " < 1000"

limit = "LIMIT 100000"

to_select = target+","+cols

SUPPORT = .005

sql = """SELECT %s FROM mapmatch_history H, sf_datasets D, (SELECT avg(%s), stddev(%s) FROM mapmatch_history) x WHERE H.dataset_id = D.id AND %s %s AND @ (%s-x.avg)/x.stddev > %f %s;""" % (to_select, target_score, target_score, target, pred, target_score, ZSCORE, limit)
print sql
cur.execute(sql)

colnames = [desc[0] for desc in cur.description]

cur_score = None
cur_rows = []

scores = []
data = []
for r in cur.fetchall():
    s = set()
    scores.append(r[0])
    for i in zip(tuple(colnames[1:]), tuple(r[1:])):
        s.add(i)
    data.append(s)

print "Analyzing %d rows" % len(data)

support_rows = SUPPORT*len(data)

frequent_itemsets = []

not_frequent = set()

def find_frequent_items(prev_set, new_k):
    frequent_round_items = defaultdict(list)
     
    for idx in range(0, len(data)):
        if idx in not_frequent:
            continue
        
        row = data[idx]
        found_support_in_row = False
        for combo in combinations(row, new_k):
            supported = True
            if new_k > 1:
                for entry in combinations(combo, new_k-1):
                    if entry not in prev_set:
                        supported = False
                        break
            if supported:
                found_support_in_row = True
                frequent_round_items[combo].append(idx)

        if not found_support_in_row:
            not_frequent.add(idx)

    return [(s, c) for (s, c) in frequent_round_items.iteritems() if len(c) > support_rows]


frequent_item_sc = []

k = 1
while True:
    frequent_item_sc = find_frequent_items(set([s for (s, c) in frequent_item_sc]), k)
    if len(frequent_item_sc) > 0:
        frequent_itemsets += frequent_item_sc
        print "pass %d found %d" % (k, len(frequent_itemsets))
        k += 1
    else:
        break

# largest itemsets first
frequent_itemsets.sort(key = lambda x: -(len(x[0])*1000+len(x[1])))

for (s, c) in frequent_itemsets:
    if len(s) <= 2:
        continue

    matching_scores = [scores[i] for i in c]
    
    print "Support: %f (%d records) avg: %.02f (std: %.02f; 5th: %.02f, 95th: %.02f)" % (float(len(c))/len(data), len(c), np.average(matching_scores), np.std(matching_scores), np.percentile(matching_scores, 5), np.percentile(matching_scores, 95))
    for item in s:
        print "%s: %s" % (item[0], item[1])

    print


json_nodes = []

for (s, c) in frequent_itemsets:
    support = (float(len(c))/len(data))
    nscores = len(c)
    matching_scores = [scores[i] for i in c]        
    avg = np.average(matching_scores)
    std = np.std(matching_scores)
    fifth = np.percentile(matching_scores, 5)
    ninetyfifth = np.percentile(matching_scores, 95)

    json_nodes.append({
            "itemset": ",".join("%s: %s" % (item[0], item[1]) for item in s),
            "support": support,
            "size": nscores,
            "avg": avg,
            "std": std,
            "5th": fifth,
            "95th": ninetyfifth})

open("apriori.json", 'w').write(json.dumps(json_nodes))

open("apriori.js", 'w').write("var title='Short Drive Times';\n var apriorijson="+json.dumps(json_nodes))

'''

fi_tree = []

# tree is a list of (set, matches, children) tuples
# returns true if inserted at a child, otherwise false
def insert(fi, tree):
    (items, matches) = fi

    for entry in tree:
        if set(items).issubset(set(entry[0])):
            if not insert(fi, entry[2]):
                entry[2].append((items, matches, []))
                return True
    return False
           

for (s, c) in frequent_itemsets:
    if not insert((s, c), fi_tree):
        fi_tree.append((s, c, []))

# make dict summary

fi_tree_summary = []

n = 0

def add(tree_level):
    json_nodes = []

    for node in tree_level:
        c = node[1]
        support = (float(len(c))/len(data))
        nscores = len(c)
        avg = np.average(matching_scores)
        std = np.std(matching_scores)
        fifth = np.percentile(matching_scores, 5)
        ninetyfifth = np.percentile(matching_scores, 95)

        json_nodes.append({
            "itemset": ",".join("%s: %s" % (item[0], item[1]) for item in node[0]),
            "support": support,
            "size": nscores,
            "avg": avg,
            "std": std,
            "5th": fifth,
            "95th": ninetyfifth,
            "children": add(node[2]) })

    return json_nodes


open("apriori.json", 'w').write(json.dumps(add(fi_tree)))
'''
