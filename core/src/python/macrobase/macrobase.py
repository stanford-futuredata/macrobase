
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
    def __init__(self):
        pass

    @abc.abstractmethod
    def process(self, batch):
        raise NotImplementedError("Calling an abstract method.")

class FeatureTransform(Operator):
    pass
    
class Classifier(Operator):
    pass


        
        
        
