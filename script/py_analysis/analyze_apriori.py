import pandas as pd
import numpy as np
import scipy
from sklearn import linear_model, cluster
from collections import defaultdict, Iterable
from itertools import chain, combinations
import operator
import psycopg2
import sys

conn = psycopg2.connect("dbname='postgres' user='pbailis' host='localhost'")

cur = conn.cursor()

cols = "hardware_manufacturer,hardware_model,hardware_carrier,android_fw_version,hardware_bootloader,start_reason,stop_reason,hidden_by_support"

target = "data_count_minutes"
pred = " < 3"

limit = "LIMIT 100000"

to_select = target+","+cols

SUPPORT = .02

sql = """
SELECT %s FROM mapmatch_history H, sf_datasets D WHERE H.dataset_id = D.id AND %s %s %s;""" % (to_select, target, pred, limit)
print sql
cur.execute(sql)

data = [c[1:] for c in cur.fetchall()]

colnames = [desc[0] for desc in cur.description]


support_rows = SUPPORT*len(data)

frequent_itemsets = []

frequent_items = defaultdict(int)

for entry in data:
    for datum in entry:
        frequent_items[datum] += 1

frequent_item_sc = [((s), c) for (s, c) in frequent_items.iteritems() if c > support_rows]

frequent_itemsets += frequent_item_sc

not_frequent = set()

def find_frequent_items(prev_set, new_k):
    frequent_round_items = defaultdict(int)
     
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
                frequent_round_items[combo] += 1

        if not found_support_in_row:
            not_frequent.add(idx)

    return [(s, c) for (s, c) in frequent_round_items.iteritems() if c > support_rows]


k = 1
while True:
    frequent_item_sc = find_frequent_items(set([s for (s, c) in frequent_item_sc]), k)
    if len(frequent_item_sc) > 0:
        frequent_itemsets += frequent_item_sc
        print "pass %d found %d (filtered rows: %d)" % (k, len(frequent_itemsets), len(not_frequent))
        k += 1
    else:
        break

# largest itemsets first
frequent_itemsets.sort(key = lambda x: -len(x[0]))

for (s, c) in frequent_itemsets:
    if len(s) <= 2:
        continue
    
    print "Support: %f (%d records)" % (float(c)/len(data), c)
    for item in s:
        print "%s: %s" % (item[0], item[1])

    print
        

