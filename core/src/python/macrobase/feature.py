
import macrobase as mb

class LambdaTransform(mb.FeatureTransform):
    def __init__(self, func, target=None, output=None):
        self.func = func
        self.target = target
        self.output = output
            
        if self.output is None:
            self.output = self._TRANSFORM_COL

    def process(self, batch):
        if self.target is None:
            self.target = [batch.columns[0]]        
        
        batch[self.output] = batch[self.target].apply(self.func)
        return batch
