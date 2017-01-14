
import abc

import pandas as pd

class Pipeline(object):
    """Core MacroBase execution unit.
       Currently implements non-branching dataflow."""
    def __init__(self):
        self.operators = []
       
    def then(self, operator):
        self.operators.append(operator)
        return self

    def add(self, operator):
        return self.then(operator)

    def process(self, data):
        for op in self.operators:
            data = op.process(data)
        return data

class Operator(object):
    
    """MacroBase dataflow operator"""    
    def __init__(self, target, output):
        pass

    """Specify target columns and output columns."""
    @abc.abstractmethod
    def process(self, batch):
        raise NotImplementedError("Calling an abstract method.")

class FeatureTransform(Operator):
    _TRANSFORM_COL = "transformed"
    pass
    
class Classifier(Operator):
    _LABEL_COL = "label"
    pass


        
        
        
