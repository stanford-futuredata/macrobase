import pandas as pd
import numpy as np
import random

NUMROWS = 100000
NUMCOLS = 100
cols = [str('A' + str(i)) for i in range(NUMCOLS)]

usage = np.random.rand(NUMROWS)
data = []
for i in range(NUMROWS):
    vals = [random.random()]
    for j in range(NUMCOLS):
        vals.append(np.random.choice(5, p=[0.7, 0.1, 0.1, 0.05, 0.05]))
    data.append(tuple(vals))

df = pd.DataFrame(data=data, columns=['usage'] + cols)
df.to_csv('generated_sample.csv',index=False, header=True)
