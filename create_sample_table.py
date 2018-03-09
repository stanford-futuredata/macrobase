import pandas as pd
import numpy as np
import random

NUMROWS = 1000000
NUMCOLS = 8
NUM_DISTINCT_VALS = 50

cols = [str('A' + str(i)) for i in range(NUMCOLS)]
dependency_col = dict([(i, np.random.choice(10)) for i in range(NUM_DISTINCT_VALS)])
# functional dependency: A4 -> A5, A6 -> A7

usage = np.random.rand(NUMROWS)
data = []
for i in range(NUMROWS):
    vals = [random.random()]
    for j in range(NUMCOLS - 3):
        vals.append(np.random.choice(NUM_DISTINCT_VALS))
    vals.append(dependency_col[vals[-1]]) # A5
    vals.append(np.random.choice(NUM_DISTINCT_VALS)) # A6
    vals.append(dependency_col[vals[-1]]) # A7
    data.append(tuple(vals))

df = pd.DataFrame(data=data, columns=['usage'] + cols)
df.to_csv('generated_sample_2-funcs_1M-copy.csv',index=False, header=True)
