import pandas as pd
import numpy as np
import random

NUMROWS = 1000000
NUMCOLS = 8
cols = [str('A' + str(i)) for i in range(NUMCOLS)]

usage = np.random.rand(NUMROWS)
data = []
for i in range(NUMROWS):
    vals = [random.random()]
    for j in range(NUMCOLS):
        vals.append(np.random.choice(50))
    data.append(tuple(vals))

df = pd.DataFrame(data=data, columns=['usage'] + cols)
df.to_csv('generated_sample.csv',index=False, header=True)
