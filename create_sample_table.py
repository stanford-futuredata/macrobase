import pandas as pd
import numpy as np
import random

NUMROWS = 100000
NUMCOLS = 8
NUM_DISTINCT_VALS = 50

cols = [str('A' + str(i)) for i in range(NUMCOLS)]
dependency_col = dict([(i, np.random.choice(10)) for i in range(NUM_DISTINCT_VALS)])
# functional dependency: A6 -> A7

usage = np.random.rand(NUMROWS)
data = []
for i in range(NUMROWS):
    vals = [random.random()]
    for j in range(NUMCOLS - 1):
        vals.append(np.random.choice(NUM_DISTINCT_VALS))
    vals.append(dependency_col[vals[-1]])
    data.append(tuple(vals))

df = pd.DataFrame(data=data, columns=['usage'] + cols)
df.to_csv('generated_sample.csv',index=False, header=True)
