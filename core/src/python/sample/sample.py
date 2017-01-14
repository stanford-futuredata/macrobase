import os
import sys
sys.path.insert(0, os.path.abspath('..'))

import pandas as pd

from macrobase import Pipeline
from macrobase.distribution import BatchMAD
from macrobase.feature import LambdaTransform

p = Pipeline()
data = pd.DataFrame([1,2,4,3,123,124,12,5109120398,12923012,123,123])

result = p.add(LambdaTransform(lambda x: x+2, output=['transform'])) \
          .then(BatchMAD(target=['transform'])).process(data)
print result

