import matplotlib.pyplot as plt

def sampling():
	data = dict()
	with open('sampling.csv', 'r') as f:
		for line in f:
			method, num_rows, sample_proportion, time = line.strip().split(', ')
			if num_rows in data:
				data[num_rows].append([method, sample_proportion, time])
			else:
				data[num_rows] = [[method, sample_proportion, time]]

	for num_rows in data:
		for method in ['reservoir', 'rejection', 'fisheryates']:
			proportion = [x[1] for x in data[num_rows] if x[0] == method]
			time = [x[2] for x in data[num_rows] if x[0] == method]
			plt.plot(proportion, time, 'o-', label=method)
		plt.legend()
		plt.title('Sampling from ' + str(num_rows))
		plt.xlabel('Sample proportion')
		plt.ylabel('Sample time (ms)')
		plt.savefig('sampling_' + str(num_rows) + '.png', format = 'png')
		plt.close()


def scoring():
	data = dict()
	with open('scoring.csv', 'r') as f:
		for line in f:
			method, num_rows, time = line.strip().split(', ')
			if method in data:
				data[method].append([num_rows, time])
			else:
				data[method] = [[num_rows, time]]

	for method in data:
		num_rows = [x[0] for x in data[method]]
		time = [x[1] for x in data[method]]
		plt.plot(num_rows, time, 'o-', label=method)
	plt.legend(loc=4)
	plt.title('Scoring times')
	plt.xlabel('Num rows')
	plt.ylabel('Scoring time (ms)')
	plt.xscale('log')
	plt.yscale('log')
	plt.savefig('scoring.png', format = 'png')
	plt.close()


def training():
	data = dict()
	with open('training.csv', 'r') as f:
		for line in f:
			method, num_rows, time = line.strip().split(', ')
			if method in data:
				data[method].append([num_rows, time])
			else:
				data[method] = [[num_rows, time]]

	for method in data:
		if method == 'train_iqr_old':
			continue
		num_rows = [x[0] for x in data[method]]
		time = [x[1] for x in data[method]]
		plt.plot(num_rows, time, 'o-', label=method)
	plt.legend()
	plt.title('Training times')
	plt.xlabel('Num rows')
	plt.ylabel('Training time (ms)')
	plt.xscale('log')
	plt.yscale('log')
	plt.savefig('training.png', format = 'png')
	plt.close()


def benchmark():
	data = dict()
	with open('benchmark.csv', 'r') as f:
		for line in f:
			method, num_cols, time = line.strip().split(', ')
			if method in data:
				data[method].append([num_cols, time])
			else:
				data[method] = [[num_cols, time]]

	for method in data:
		num_cols = [x[0] for x in data[method]]
		time = [x[1] for x in data[method]]
		plt.plot(num_cols, time, 'o-', label=method)
	plt.legend()
	plt.title('Classification times on 1M rows')
	plt.xlabel('Num cols')
	plt.ylabel('Classification time (ms)')
	# plt.xscale('log')
	# plt.yscale('log')
	plt.savefig('benchmark.png', format = 'png')
	plt.close()


def main():
    sampling()
    # scoring()
    # training()
    # benchmark()


if __name__ == "__main__":
    main()