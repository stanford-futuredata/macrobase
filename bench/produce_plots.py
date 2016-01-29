import matplotlib.pyplot as plt
import sys

workloads_to_be_plotted = [
  "testTasks",
  "testTasksStreaming"
]

timing_types = [
  'Loading',
  'Summarization',
  'Scoring',
  'Training'
]

def parse_output_file(filename):
  parameters_description = None
  parsed_results = dict()
  with open(filename, 'r') as f:
    lines = f.read().split('\n')
    for i in xrange(len(lines)):
      line = lines[i]
      if "Running all workloads" in line:
        parameters_description = line.split("Running all workloads with ")[1].strip()
      if "Times" in line:
        split_point = line.find(": ")
        times = eval(line[split_point+2:])

        itemsets_line = lines[i+1].split(" , ")
        iterations_line = lines[i+2].split(" , ")

        itemsets_list_line = lines[i+3]
        itemsets_list_str = itemsets_list_line.replace("Union of all itemsets:", "")
        itemsets_list = eval(itemsets_list_str)

        itemsets_mean = float(itemsets_line[0].split(": ")[1])
        itemsets_stddev = float(itemsets_line[1].split(": ")[1])

        iterations_mean = float(iterations_line[0].split(": ")[1])
        iterations_stddev = float(iterations_line[1].split(": ")[1])

        workload_name = lines[i-1].split("-->")[0].strip()

        tps_line = lines[i+4]
        tps_line = tps_line.split(" , ")

        tps_mean = float(tps_line[0].split(": ")[1])
        tps_stddev = float(tps_line[1].split(": ")[1])

        try:
          [parameter_type, parameter_value] = parameters_description.split(" = ")
          if parameter_type not in parsed_results:
            parsed_results[parameter_type] = dict()
            parsed_results[parameter_type][workload_name] = dict()
          if workload_name not in parsed_results[parameter_type]:
            parsed_results[parameter_type][workload_name] = dict()
          parsed_results[parameter_type][workload_name][parameter_value] = (times, (itemsets_mean, itemsets_stddev), (iterations_mean, iterations_stddev), itemsets_list, (tps_mean, tps_stddev))
        except:
          parameter_type = parameters_description
          if parameter_type not in parsed_results:
            parsed_results[parameter_type] = dict()
          parsed_results[parameter_type][workload_name] = (times, (itemsets_mean, itemsets_stddev), (iterations_mean, iterations_stddev), itemsets_list, (tps_mean, tps_stddev))
  return parsed_results

def get_time(parsed_results, parameter_type, workload_name, parameter_value, timing_type):
  if timing_type == 'Total':
    tot_time = 0.0
    for timing_type_prime in timing_types:
      tot_time += parsed_results[parameter_type][workload_name][parameter_value][0][timing_type_prime.lower()][0]
    return (tot_time, 0.0)
  return parsed_results[parameter_type][workload_name][parameter_value][0][timing_type.lower()]

def compute_precision_and_recall(itemsets, ground_truth_itemsets):
  num_correct = 0
  for itemset in itemsets:
    if itemset in ground_truth_itemsets:
      num_correct += 1
  precision = float(num_correct) / float(len(itemsets))

  num_correct = 0
  for itemset in ground_truth_itemsets:
    if itemset in itemsets:
      num_correct += 1
  recall = float(num_correct) / float(len(ground_truth_itemsets))

  return precision, recall

def plot_time_graphs(parsed_results, plots_dir):
  for parameter_type in parsed_results:
    if parameter_type == "defaultParameters":
      continue
    for timing_type in timing_types + ['Total']:
      try:
        plt.cla()
        plt.xscale('log')
        handles = list()
        for workload_name in parsed_results[parameter_type]:
          if workload_name not in workloads_to_be_plotted:
            continue
          keys = list()
          values = list()
          stddevs = list()
          for parameter_value in sorted(parsed_results[parameter_type][workload_name].keys()):
            try:
              value, stddev = get_time(parsed_results, parameter_type, workload_name, parameter_value, timing_type)
              values.append(value)
              stddevs.append(stddev)
              keys.append(parameter_value)
            except:
              continue
          handle = plt.errorbar(keys, values, yerr=stddevs, label=workload_name, marker='o')
          handles.append(handle)
        lgd = plt.legend(handles=handles, loc=(0.0, -1.5))
        plt.xlabel(parameter_type)
        plt.ylabel(timing_type + " time (in milliseconds)")
        plt.savefig(plots_dir + "/ " + parameter_type + "_" + timing_type + '.pdf', bbox_extra_artists=(lgd,), bbox_inches='tight')
      except:
        continue

def plot_aux_graphs(parsed_results, idx, ylabel, modifier, plots_dir):
  for parameter_type in parsed_results:
    if parameter_type == "defaultParameters":
      continue
    plt.cla()
    plt.xscale('log')
    handles = list()
    for workload_name in parsed_results[parameter_type]:
      if workload_name not in workloads_to_be_plotted:
        continue
      keys = list()
      values = list()
      stddevs = list()
      for parameter_value in sorted(parsed_results[parameter_type][workload_name].keys()):
        try:
          (value, stddev) = parsed_results[parameter_type][workload_name][parameter_value][idx]
          values.append(value)
          stddevs.append(stddev)
          keys.append(parameter_value)
        except:
          continue
      handle = plt.errorbar(keys, values, yerr=stddevs, label=workload_name, marker='o')
      handles.append(handle)
    lgd = plt.legend(handles=handles, loc=(0.0, -1.5))
    plt.xlabel(parameter_type)
    plt.ylabel(ylabel)
    plt.savefig(plots_dir + "/" + parameter_type + "_" + modifier + ".pdf", bbox_extra_artists=(lgd,), bbox_inches='tight')

def plot_recall_precision(parsed_results, idx, ylabel, modifier, plots_dir):
  for parameter_type in parsed_results:
    if parameter_type == "defaultParameters":
      continue
    plt.cla()
    plt.xscale('log')
    handles = list()
    for workload_name in parsed_results[parameter_type]:
      if workload_name not in workloads_to_be_plotted:
        continue
      keys = list()
      values = list()
      for parameter_value in sorted(parsed_results[parameter_type][workload_name].keys()):
        try:
          itemsets = parsed_results[parameter_type][workload_name][parameter_value][3]
          ground_truth_itemsets = parsed_results["defaultParameters"][workload_name][3]
          value = compute_precision_and_recall(itemsets, ground_truth_itemsets)[idx]
          values.append(value)
          keys.append(parameter_value)
        except:
          continue
      handle, = plt.plot(keys, values, label=workload_name , marker='o')
      handles.append(handle)
    lgd = plt.legend(handles=handles, loc=(0.0, -1.5))
    plt.xlabel(parameter_type)
    plt.ylabel(ylabel)
    plt.savefig(plots_dir + "/" + parameter_type + "_" + modifier + ".pdf", bbox_extra_artists=(lgd,), bbox_inches='tight')

if __name__ == '__main__':
  if (len(sys.argv) < 3):
    print "Incorrect usage: python %s <output_file> <plot_directory>" % sys.argv[0]
    exit(-1)
  parsed_results = parse_output_file(sys.argv[1])
  plot_time_graphs(parsed_results, sys.argv[2])
  plot_aux_graphs(parsed_results, 1, "Number of itemsets", "Itemsets", sys.argv[2])
  plot_aux_graphs(parsed_results, 2, "Number of iterations in MCD step", "IterationsMCD", sys.argv[2])
  plot_recall_precision(parsed_results, 0, "Precision", "Precision", sys.argv[2])
  plot_recall_precision(parsed_results, 1, "Recall", "Recall", sys.argv[2])
  plot_aux_graphs(parsed_results, 4, "Throughput in tuples/sec", "Tps", sys.argv[2])
