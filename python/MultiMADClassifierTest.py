from MultiMADClassifier import *
from timeit import default_timer as timer

NUM_TRIALS = 1

def test():
	# read in data
	with open('../lib/src/test/resources/hepmass.csv', 'r') as f:
		_ = f.readline()
		rows = []
		for _ in xrange(1000000):
			data = [float(x) for x in f.next().strip().split(',')[1:]]
			data.extend(data)
			data.extend(data)
			rows.append(data)
		input = np.asarray(rows).transpose()

	# process
	mad = MultiMADClassifier()
	
	start = timer()
	for _ in xrange(NUM_TRIALS):
		mad.process(input)
	end = timer()
	print "score: {}".format(mad.scoreTime)
	print "Time elapsed: {}".format(end - start)
	classification1 = mad.classification

	mad.scoreTime = 0

	start = timer()
	for _ in xrange(NUM_TRIALS):
		mad.processWeld(input)
	end = timer()
	print "scoreWeld: {}".format(mad.scoreTime)
	print "Time elapsed: {}".format(end - start)

	classification2 = mad.classification
	# print "{}".format(sum(classification1[1] == classification2[1]))

if __name__ == "__main__":
	test()