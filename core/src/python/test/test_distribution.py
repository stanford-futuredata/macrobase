import os
import sys
sys.path.insert(0, os.path.abspath('..'))

import unittest
from pandas.util.testing import assert_series_equal

from macrobase import Pipeline
from macrobase.distribution import BatchMAD

import pandas as pd
import numpy as np

class TestMAD(unittest.TestCase):
    def test(self):
        s = pd.DataFrame([1.5, 50, 2, 3, 10000])

        m = BatchMAD()
        result = m.process(s)
        assert result[BatchMAD._LABEL_COL].equals(pd.Series([False, True, False, False, True]))
