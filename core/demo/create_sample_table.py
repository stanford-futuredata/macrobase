import pandas as pd
import numpy as np
import random

NUMROWS = 10000000
cols = [str(i) for i in range(10)]

usage = np.random.rand(NUMROWS)
data = []
for i in range(NUMROWS):
    a = random.randint(0, 10)
    b = random.randint(0, 10)
    c = random.randint(0, 10)
    d = random.randint(0, 10)
    e = random.randint(0, 10)
    data.append((random.random(), a, b, c, d, e))
    if i % 1000000 == 999999:
        print i + 1

df = pd.DataFrame(data=data, columns=['usage', 'A', 'B', 'C', 'D', 'E'])
df.to_csv('generated_sample.csv',index=False, header=True)
