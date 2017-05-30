from MultiMADClassifier import *
from timeit import default_timer as timer
import numpy as np

NUM_TRIALS = 10

def test():
	# read in data
	# with open('../lib/src/test/resources/hepmass.csv', 'r') as f:
	# 	_ = f.readline()
	# 	rows = []
	# 	for _ in xrange(1000000):
	# 		data = [float(x) for x in f.next().strip().split(',')[1:]]
	# 		data.extend(data)
	# 		data.extend(data)
	# 		rows.append(data)
	# 	input = np.asarray(rows).transpose()

	# use synthetic data
	num_cols = 20
	num_rows = 1000000
	input = np.random.rand(num_cols, num_rows)

	# process
	mad = MultiMADClassifier()
	
	start = timer()
	for _ in xrange(NUM_TRIALS):
		mad.process(input)
	end = timer()
	print "Without Weld:"
	print "Scoring time: {}".format(mad.scoreTime)
	print "Total time elapsed: {}".format(end - start)
	classification1 = mad.classification

	mad.scoreTime = 0

	start = timer()
	for _ in xrange(NUM_TRIALS):
		mad.processWeld(input, False)
	end = timer()
	print "Using Weld, only encoding/decoding:"
	print "Scoring time: {}".format(mad.scoreTime)
	print "Total time elapsed: {}".format(end - start)

	mad.scoreTime = 0

	start = timer()
	for _ in xrange(NUM_TRIALS):
		mad.processWeld(input, True)
	end = timer()
	print "Using Weld:"
	print "Scoring time: {}".format(mad.scoreTime)
	print "Total time elapsed: {}".format(end - start)

	classification2 = mad.classification
	print "Classifications agree: {} out of {}".format(
		sum(classification1[1] == classification2[1]), num_rows)

if __name__ == "__main__":
	test()