import pandas as pd
import numpy as np
from sklearn import linear_model, cluster
from collections import defaultdict, Iterable
from itertools import chain, combinations
import operator
import psycopg2
import matplotlib.pyplot as plt

conn = psycopg2.connect("dbname='postgres' user='pbailis' host='localhost'")

cur = conn.cursor()

cols = "hardware_manufacturer,hardware_model,hardware_carrier,android_fw_version,hardware_bootloader"

target = "battery_drain_rate_per_hour"
pred = "< 1000"

limit = "LIMIT 10000"

to_select = target+","+cols

sql = """
SELECT %s FROM mapmatch_history H, sf_datasets D WHERE H.dataset_id = D.id AND %s %s %s;""" % (to_select, target, pred, limit)
print sql
cur.execute(sql)

colnames = [desc[0] for desc in cur.description]

cur_score = None
cur_rows = []

df = None

data = pd.DataFrame(cur.fetchall(), columns=colnames)
features = data.drop(target, 1)

pd.set_option('display.max_rows', len(features))
pd.set_option('expand_frame_repr', False)

plt.figure()
data[target].apply(lambda x: x).plot(kind='hist', bins=200)
plt.show()
