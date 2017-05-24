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

	def subtract(self, number):
		template = "map({0}, |e| e - {1})"
		self.weldobj.weld_code = template.format(self.weldobj.weld_code, str(number))
		return self

	def divide(self, number):
		template = "map({0}, |e| e / {1})"
		self.weldobj.weld_code = template.format(self.weldobj.weld_code, str(number))
		return self

	def absoluteValue(self):
		template = "map({0}, |e| if(e > 0.0, e, -e))"
		self.weldobj.weld_code = template.format(self.weldobj.weld_code)
		return self

	def cutoff(self, number):
		template = "map({0}, |e| if(e > {1}, 1.0, 0.0))"
		self.weldobj.weld_code = template.format(self.weldobj.weld_code, str(number))
		return self

	def everything(self, median, MAD, cutoff):
		template = "map({0}, |e| if((e - {1}) / {2} > {3} || (e - {1}) / {2} < -{3}, 1.0, 0.0))"
		# template = "map({0}, |e| if(if(e - {1} < 0.0, {1} - e, e - {1}) / {2} > {3}, 1.0, 0.0))"
		self.weldobj.weld_code = template.format(self.weldobj.weld_code, str(median), str(MAD), str(cutoff))
		return self

	def nothing(self):
		# template = "map({0}, |e| e)"
		# self.weldobj.weld_code = template.format(self.weldobj.weld_code)
		return self

	def eval(self):
		v = self.weldobj.evaluate(WeldVec(WeldDouble()), verbose=False)
		return v