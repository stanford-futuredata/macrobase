import pandas as pd
import numpy as np
from sklearn import linear_model

import psycopg2

examine_cols = ["hardware_manufacturer", "hardware_model", "hardware_build", "hardware_carrier", "android_fw_version", "android_api_version"]
target_col = ["data_count_minutes"]

all_cols = examine_cols + target_col

conn = psycopg2.connect("dbname='postgres' user='pbailis' host='localhost'")

cur = conn.cursor()

sql = "SELECT %s FROM mapmatch_history H, sf_datasets D WHERE H.dataset_id = D.id AND H.data_count_accel_samples < 10000060 LIMIT 10000" % (",".join(all_cols))
cur.execute(sql)

colnames = [desc[0] for desc in cur.description]
print colnames

data = []
scores = []
nrows = 0
for row in cur.fetchall():
    nrows += 1
    data.append(row[:-1])
    scores.append(1./max(row[-1], .0001))

df = pd.DataFrame(data, columns=colnames[:-1])

dummies = []
for c in examine_cols:
    dummies.append(pd.get_dummies(df[c], prefix=c))

dummied = reduce(lambda x,y: x.join(y), dummies)

regr = linear_model.LinearRegression()
regr.fit(dummied, scores)
print nrows
print regr.coef_
print max(regr.coef_)






