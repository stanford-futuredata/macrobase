import numpy as np
from weld.weldobject import *
from weld.types import *
from weld.encoders import NumpyArrayEncoder, NumpyArrayDecoder

class WeldVector(object):
	def __init__(self, vector):
		self.vector = vector
		self.weldobj = WeldObject(NumpyArrayEncoder(), NumpyArrayDecoder())
		name = self.weldobj.update(vector, WeldVec(WeldDouble()))
		self.weldobj.weld_code = name

	def negate(self):
		template = "map({0}, |e| -e)"
		self.weldobj.weld_code = template.format(self.weldobj.weld_code)
		return self

	def eval(self):
		v = self.weldobj.evaluate(WeldVec(WeldDouble()), verbose=False)
		return v

def main():
	metrics = np.asarray([1.0, 2.0, 3.0])
	vector = WeldVector(metrics)
	results = vector.negate().eval()

if __name__ == "__main__":
	main()