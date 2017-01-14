
import macrobase as mb
import pandas as pd

class BatchMAD(mb.Classifier):
    ''' Batch classifier computes Median Absolute Deviation
        and compares to specified threshold '''
    def __init__(self, threshold = 3):
        self.threshold = threshold
        
    def process(self, batch):
        self.median = batch.median()
        self.mad = (abs(batch - self.median)).median()
        return pd.DataFrame(abs(batch-self.median)/self.mad > self.threshold)
