
import macrobase as mb

class LambdaTransform(mb.FeatureTransform):
    def __init__(self, func):
        self.func = func
        
    def process(self, batch):
        return batch.apply(self.func)
