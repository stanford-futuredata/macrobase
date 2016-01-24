import os

from config_parameters import all_config_parameters
from sweeping_parameters import sweeping_parameters
from time import strftime

testing_dir = "workflows"
batch_template_conf_file = "batch_template.conf"
streaming_template_conf_file = "streaming_template.conf"

NUM_RUNS_PER_WORKFLOW = 5

default_args = {
  "minInlierRatio": 1.0,
  "minSupport": 0.001,

  "usePercentile": "true",
  "targetPercentile": 0.99,
  "useZScore": "false",
  "zScore": 3.0,

  "inputReservoirSize": 10000,
  "scoreReservoirSize": 10000,
  "inlierItemSummarySize": 1000,
  "outlierItemSummarySize": 10000,
  "summaryRefreshPeriod": 100000,
  "modelRefreshPeriod": 10000,

  "useRealTimePeriod": "false",
  "useTupleCountPeriod": "true",

  "warmupCount": 1000,
  "decayRate": 0.01,

  "alphaMCD": 0.5,
  "stoppingDeltaMCD": 0.001
}

def process_config_parameters(config_parameters):
  for config_parameter_type in config_parameters:
    if type(config_parameters[config_parameter_type]) == list:
      config_parameters[config_parameter_type] = ", ".join([str(para) for para in config_parameters[config_parameter_type]])

def create_config_file(config_parameters, conf_file):
  template_conf_file = batch_template_conf_file if config_parameters["isBatchJob"] else streaming_template_conf_file
  template_conf_contents = open(template_conf_file, 'r').read()
  conf_contents = template_conf_contents % config_parameters
  with open(conf_file, 'w') as f:
    f.write(conf_contents)

def parse_results(results_file):
  times = dict()
  num_itemsets = 0
  num_iterations = 0
  itemsets = list()
  with open(results_file, 'r') as f:
    lines = f.read().split('\n')
    for i in xrange(len(lines)):
      line = lines[i]
      if line.startswith("DEBUG"):
        if "time" in line:
          line = line.split("...ended")[1].strip()
          line_tokens = line.split("(")
          time_type = line_tokens[0].strip()
          time = int(line_tokens[1][6:-4])
          times[time_type] = time
        elif "itemsets" in line:
          line = line.split("Number of itemsets:")[1].strip()
          num_itemsets = int(line)
        elif "iterations" in line:
          line = line.split("Number of iterations in MCD step:")[1].strip()
          num_iterations = int(line)
      if "Columns" in line:
        j = i + 1
        itemset = dict()
        while lines[j].strip() != '':
          itemset_str = lines[j].lstrip()
          [itemset_type, itemset_value] = itemset_str.split(": ")
          if itemset_type != "" and itemset_value != "":
            itemset[itemset_type] = itemset_value
          j += 1
        if itemset != {}:
          itemsets.append(itemset)
  return times, num_itemsets, num_iterations, itemsets

def get_stats(value_list):
  value_list = [float(value) for value in value_list]
  mean = sum(value_list) / len(value_list)
  stddev = (sum([(value - mean)**2 for value in value_list]) / len(value_list)) ** 0.5
  return mean, stddev

def run_workload(config_parameters, print_itemsets=True):
  sub_dir = os.path.join(os.getcwd(), testing_dir, config_parameters["taskName"], strftime("%m-%d-%H:%M:%S"))
  os.system("mkdir -p %s" % sub_dir)
  process_config_parameters(config_parameters)
  conf_file = "batch.conf" if config_parameters["isBatchJob"] else "streaming.conf"
  conf_file = os.path.join(sub_dir, conf_file)
  results_file = os.path.join(sub_dir, "results.txt")
  create_config_file(config_parameters, conf_file)
  cmd = "batch" if config_parameters["isBatchJob"] else "streaming"

  all_times = dict()
  all_num_itemsets = list()
  all_num_iterations = list()
  all_itemsets = set()

  for i in xrange(NUM_RUNS_PER_WORKFLOW):
    os.system("cd ..; java ${JAVA_OPTS} -cp \"src/main/resources/:target/classes:target/lib/*:target/dependency/*\" macrobase.MacroBase %s %s > %s" % (cmd, conf_file, results_file))
    times, num_itemsets, num_iterations, itemsets = parse_results(results_file)

    for time_type in times:
      if time_type not in all_times:
        all_times[time_type] = list()
      all_times[time_type].append(times[time_type])

    all_num_itemsets.append(num_itemsets)
    all_num_iterations.append(num_iterations)
    for itemset in itemsets:
      all_itemsets.add(frozenset(itemset.items()))

  mean_and_stddev_times = dict()
  for time_type in all_times:
    mean_and_stddev_times[time_type] = get_stats(all_times[time_type])
  mean_num_itemsets, stddev_num_itemsets = get_stats(all_num_itemsets)
  mean_num_iterations, stddev_num_iterations = get_stats(all_num_iterations)

  print config_parameters["taskName"], "-->"
  print "Times:", mean_and_stddev_times
  print "Mean number of itemsets:", mean_num_itemsets, ", Stddev number of itemsets:", stddev_num_itemsets
  print "Mean number of iterations:", mean_num_iterations, ", Stddev number of iterations:", stddev_num_iterations
  if print_itemsets:
    print "Union of all itemsets:", list(all_itemsets)

def run_all_workloads(sweeping_parameter_name=None, sweeping_parameter_value=None):
  if sweeping_parameter_name is not None:
    print "Running all workloads with", sweeping_parameter_name, "=", sweeping_parameter_value
  else:
    print "Running all workloads with default parameters"
  print
  for config_parameters_raw in all_config_parameters:
    config_parameters = {}
    for key in default_args:
      config_parameters[key] = default_args[key]
    for key in config_parameters_raw:
      config_parameters[key] = config_parameters_raw[key]
    if sweeping_parameter_name is not None:
      config_parameters[sweeping_parameter_name] = sweeping_parameter_value

    run_workload(config_parameters)
  print

if __name__ == '__main__':
  for sweeping_parameter_name in sweeping_parameters:
    for sweeping_parameter_value in sweeping_parameters[sweeping_parameter_name]:
      run_all_workloads(sweeping_parameter_name, sweeping_parameter_value)
