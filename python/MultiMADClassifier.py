import numpy as np
# from weld_vector import WeldVector
from weld_matrix import WeldMatrix
from timeit import default_timer as timer

class MultiMADClassifier:
	MAD_TO_ZSCORE_COEFFICIENT = 1.4826;

	def __init__(self):
		self.median = 0
		self.MAD = 0
		self.medians = []
		self.MADs = []
		self.zscore_coefficient = 0
		self.cutoff = 2.576
		self.classification = []
		self.scoreTime = 0

	def process(self, input):
		for row in xrange(input.shape[0]):
			metrics = input[row,:]
			self.median, self.MAD = self.computeStatistics(metrics)
			self.zscore_coefficient = self.MAD * self.MAD_TO_ZSCORE_COEFFICIENT
			start = timer()
			results = self.score(metrics)
			self.scoreTime += (timer() - start)
			self.classification.append(results)

	def processWeld(self, input, scoring=True):
		self.medians = []
		self.MADs = []
		for row in xrange(input.shape[0]):
			metrics = input[row,:]
			self.median, self.MAD = self.computeStatistics(metrics)
			self.medians.append(self.median)
			self.MADs.append(self.MAD * self.MAD_TO_ZSCORE_COEFFICIENT)
		start = timer()
		matrix = WeldMatrix(input, np.asarray(self.medians), np.asarray(self.MADs))
		if scoring:
			self.classification = matrix.subtract().absoluteValue().divide().cutoff(self.cutoff).eval()
		else:
			self.classification = matrix.eval()
		self.scoreTime += (timer() - start)

	def computeStatistics(self, metrics):
		median = np.median(metrics)
		residuals = np.abs(metrics - median)
		MAD = np.median(residuals)
		if MAD == 0:
			MAD = np.sum(residuals)/len(residuals)
		return median, MAD

	def score(self, metrics):
		metrics = np.abs(metrics - self.median) / self.zscore_coefficient
		results = metrics >= self.cutoff
		return results

	### Old code ###
	# def processWeldOld(self, input):
	# 	for row in xrange(input.shape[0]):
	# 		metrics = input[row,:]
	# 		self.median, self.MAD = self.computeStatistics(metrics)
	# 		self.zscore_coefficient = self.MAD * self.MAD_TO_ZSCORE_COEFFICIENT
	# 		start = timer()
			
	# 		vector = WeldVector(metrics)
	# 		# results = vector.subtract(self.median).absoluteValue().divide(self.zscore_coefficient).cutoff(self.cutoff).eval()
	# 		results = vector.everything(self.median, self.zscore_coefficient, self.cutoff).eval()
			
	# 		self.scoreTime += (timer() - start)
	# 		self.classification.append(results)
