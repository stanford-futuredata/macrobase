import argparse
import json
import os

from time import strftime


testing_dir = "workflows"
batch_template_conf_file = "batch_template.conf"
streaming_template_conf_file = "streaming_template.conf"

default_args = {
  "minInlierRatio": 3.0,
  "minSupport": 0.001,

  "usePercentile": "true",
  "targetPercentile": 0.99,
  "useZScore": "false",
  "dbUser": os.getenv('USER', None),
  "dbPassword": None,
  "zScore": 3.0,

  "inputReservoirSize": 10000,
  "scoreReservoirSize": 10000,
  "inlierItemSummarySize": 10000,
  "outlierItemSummarySize": 10000,
  "summaryRefreshPeriod": 100000,
  "modelRefreshPeriod": 100000,

  "useRealTimePeriod": "false",
  "useTupleCountPeriod": "true",

  "warmupCount": 50000,
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
  tuples_per_second = 0.0
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
        elif "Tuples / second" in line:
          line = line.split("Tuples / second = ")[1]
          tuples_per_second = float(line.split("tuples / second")[0].strip())
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
  return times, num_itemsets, num_iterations, itemsets, tuples_per_second


def get_stats(value_list):
  value_list = [float(value) for value in value_list]
  mean = sum(value_list) / len(value_list)
  stddev = (sum([(value - mean)**2 for value in value_list]) / len(value_list)) ** 0.5
  return mean, stddev


def run_workload(config_parameters, number_of_runs, print_itemsets=True):
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
  all_tuples_per_second = list()

  for i in xrange(number_of_runs):
    macrobase_cmd = '''java ${{JAVA_OPTS}} \\
        -cp "src/main/resources/:target/classes:target/lib/*:target/dependency/*" \\
        macrobase.MacroBase {cmd} {conf_file} \\
        > {results_file}'''.format(
        cmd=cmd, conf_file=conf_file, results_file=results_file)
    print 'running the following command:'
    print macrobase_cmd
    os.system("cd ..; %s" % macrobase_cmd)
    times, num_itemsets, num_iterations, itemsets, tuples_per_second = parse_results(results_file)

    for time_type in times:
      if time_type not in all_times:
        all_times[time_type] = list()
      all_times[time_type].append(times[time_type])

    all_num_itemsets.append(num_itemsets)
    all_num_iterations.append(num_iterations)
    for itemset in itemsets:
      all_itemsets.add(frozenset(itemset.items()))
    all_tuples_per_second.append(tuples_per_second)

  mean_and_stddev_times = dict()
  for time_type in all_times:
    mean_and_stddev_times[time_type] = get_stats(all_times[time_type])
  mean_num_itemsets, stddev_num_itemsets = get_stats(all_num_itemsets)
  mean_num_iterations, stddev_num_iterations = get_stats(all_num_iterations)
  mean_tuples_per_second, stddev_tuples_per_second = get_stats(all_tuples_per_second)

  print config_parameters["taskName"], "-->"
  print "Times:", mean_and_stddev_times
  print "Mean number of itemsets:", mean_num_itemsets, ", Stddev number of itemsets:", stddev_num_itemsets
  print "Mean number of iterations:", mean_num_iterations, ", Stddev number of iterations:", stddev_num_iterations
  if print_itemsets:
    print "Union of all itemsets:", list(all_itemsets)
  print "Mean tuples / second:", mean_tuples_per_second, ", Stddev tuples / second:", stddev_tuples_per_second


def run_all_workloads(configurations, defaults, number_of_runs,
                      sweeping_parameter_name=None,
                      sweeping_parameter_value=None):
  if sweeping_parameter_name is not None:
    print "Running all workloads with", sweeping_parameter_name, "=", sweeping_parameter_value
  else:
    print "Running all workloads with defaultParameters"
  print

  for config_parameters_raw in configurations:
    config_parameters = defaults.copy()
    config_parameters.update(config_parameters_raw)
    if sweeping_parameter_name is not None:
      config_parameters[sweeping_parameter_name] = sweeping_parameter_value

    run_workload(config_parameters, number_of_runs)
  print


def _add_camel_case_argument(parser, argument, **kwargs):
  """
  Add an argument with a destination attribute being camel cased version of the
  argument instead of underscore separated version.
  """
  camel_case_str = ''.join([substr.title() if index != 0 else substr
                            for index, substr in enumerate(
                                argument.strip('-').split('-'))])
  parser.add_argument(argument, dest=camel_case_str, **kwargs)


def parse_args():
  parser = argparse.ArgumentParser()
  parser.add_argument('--configurations-file',
                      type=argparse.FileType('r'),
                      default='conf/all_configuration_parameters.json',
                      help='File with a list of configuration parameters')
  parser.add_argument('--sweeping-parameters-file',
                      type=argparse.FileType('r'),
                      default='conf/sweeping_parameters.json',
                      help='File with a dictionary of sweeping parameters')
  parser.add_argument('--number-of-runs', default=5, type=int,
                      help='Number of times to run a workload with same '
                           'parameters.')
  parser.add_argument('--compile', action='store_true',
                      help='Run mvn compile before running java code.')
  _add_camel_case_argument(parser, '--db-user')
  _add_camel_case_argument(parser, '--db-password')
  args = parser.parse_args()
  args.configurations = json.load(args.configurations_file)
  args.sweeping_parameters = json.load(args.sweeping_parameters_file)
  return args


if __name__ == '__main__':
  args = parse_args()

  if args.compile:
    status = os.system('cd .. && mvn compile')
    if status != 0:
      os._exit(status)

  # Update default values if script argument is provided
  defaults = default_args.copy()
  for key, default_override in vars(args).items():
    if default_override is not None and key in defaults:
      defaults[key] = default_override

  run_all_workloads(args.configurations, defaults, args.number_of_runs)
  for parameter_name, values in args.sweeping_parameters.iteritems():
    for parameter_value in values:
      run_all_workloads(args.configurations, defaults, args.number_of_runs,
                        sweeping_parameter_name=parameter_name,
                        sweeping_parameter_value=parameter_value)
