import numpy as np
from weld.weldobject import *
from weld.types import *
from weld.encoders import *
from grizzly.encoders import *


class WeldMatrix(object):
	def __init__(self, matrix, medians, MADs):
		self.matrix = matrix
		self.weldobj = WeldObject(NumPyEncoder(), NumPyDecoder())
		self.medians = self.weldobj.update(medians, WeldVec(WeldDouble()))
		self.MADs = self.weldobj.update(MADs, WeldVec(WeldDouble()))
		name = self.weldobj.update(matrix, WeldVec(WeldVec(WeldDouble())))
		self.weldobj.weld_code = name

	def subtract(self):
		template = "result(for({0}, appender[vec[f64]], |b,i,e| merge(b, map(e, |f| f - lookup({1}, i)))))"
		self.weldobj.weld_code = template.format(self.weldobj.weld_code, self.medians)
		return self

	def divide(self):
		template = "result(for({0}, appender[vec[f64]], |b,i,e| merge(b, map(e, |f| f / lookup({1}, i)))))"
		self.weldobj.weld_code = template.format(self.weldobj.weld_code, self.MADs)
		return self

	def absoluteValue(self):
		template = "map({0}, |e| map(e, |f| if(f > 0.0, f, -f)))"
		self.weldobj.weld_code = template.format(self.weldobj.weld_code)
		return self

	def cutoff(self, number):
		template = "map({0}, |e| map(e, |f| if(f > {1}, 1.0, 0.0)))"
		self.weldobj.weld_code = template.format(self.weldobj.weld_code, str(number))
		return self

	def eval(self):
		v = self.weldobj.evaluate(WeldVec(WeldVec(WeldDouble())), verbose=False)
		return v

	### Old code ###
	def subtractParRow(self):
		template = "map({0}, |e| result(for(e, appender[f64], |b,i,f| merge(b, f - lookup({1}, i)))))"
		self.weldobj.weld_code = template.format(self.weldobj.weld_code, self.medians)
		return self

	def divideParRow(self):
		template = "map({0}, |e| result(for(e, appender[f64], |b,i,f| merge(b, f / lookup({1}, i)))))"
		self.weldobj.weld_code = template.format(self.weldobj.weld_code, self.MADs)
		return self

	def reduce(self):
		template = "result(for(iter({0}, 0L, 1L, 1L), appender[vec[f64]], |b,i,e| merge(b, e)))"
		self.weldobj.weld_code = template.format(self.weldobj.weld_code)
		return self

	def eval1D(self):
		v = self.weldobj.evaluate(WeldVec(WeldDouble()), verbose=False)
		return v

	def nothing(self):
		v = self.weldobj.evaluate(WeldVec(WeldVec(WeldDouble())), verbose=False)
		return v