
import macrobase as mb
import pandas as pd

class BatchMAD(mb.Classifier):
    ''' Batch classifier computes Median Absolute Deviation
        and compares to specified threshold '''
    def __init__(self, target=None, output=None, threshold = 3):
        self.threshold = threshold
        self.target = target
        self.output = output
        
        if self.output is None:
            self.output = self._LABEL_COL

    """Specify target column(s) and output column."""    
    def process(self, batch):
        if self.target is None:
            self.target = [batch.columns[0]]        
        
        self.median = batch[self.target].median()
        self.mad = (abs(batch[self.target] - self.median)).median()
        batch[self.output] = (abs(batch[self.target]-self.median)/self.mad
                               > self.threshold)
        return batch
