import pandas as pd
import numpy as np
import random

NUMROWS = 1000000
locationVal = ['USA', 'CAN', 'GRB', 'GER', 'CHN', 'JPN', 'MEX']
versionVal = ['v1', 'v2', 'v3', 'v4', 'v5', 'v6']
nameVal = ['A', 'B', 'C', 'D', 'E']

usage = np.random.rand(NUMROWS)
data = []
for i in range(NUMROWS):
    l = random.randint(0, len(locationVal) - 1)
    v = random.randint(0, len(versionVal) - 1)
    n = random.randint(0, len(nameVal) - 1)
    data.append((usage[i], locationVal[l], versionVal[v], nameVal[n]))

df = pd.DataFrame(data=data, columns=['usage', 'location', 'version', 'name'])
df.to_csv('generated_sample.csv',index=False, header=True)
