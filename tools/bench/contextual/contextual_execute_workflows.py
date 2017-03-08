import argparse
import json
import os

from time import strftime


testing_dir = "workflows"
batch_template_conf_file = "contextual_batch_template.conf"
streaming_template_conf_file = "contextual_streaming_template.conf"

default_args = {
  "macrobase.analysis.minOIRatio": 3.0,
  "macrobase.analysis.minSupport": 0.5,

  "macrobase.analysis.usePercentile": "true",
  "macrobase.analysis.targetPercentile": 0.99,
  "macrobase.analysis.useZScore": "false",
  "macrobase.loader.db.user": os.getenv('USER', None),
  "macrobase.loader.db.password": None,
  "macrobase.analysis.zscore.threshold": 5.0,

  "macrobase.analysis.streaming.inputReservoirSize": 10000,
  "macrobase.analysis.streaming.scoreReservoirSize": 10000,
  "macrobase.analysis.streaming.inlierItemSummarySize": 10000,
  "macrobase.analysis.streaming.outlierItemSummarySize": 10000,
  "macrobase.analysis.streaming.summaryUpdatePeriod": 100000,
  "macrobase.analysis.streaming.modelUpdatePeriod": 100000,

  "macrobase.analysis.streaming.useRealTimePeriod": "false",
  "macrobase.analysis.streaming.useTupleCountPeriod": "true",

  "macrobase.analysis.streaming.warmupCount": 50000,
  "macrobase.analysis.streaming.decayRate": 0.01,

  "macrobase.analysis.mcd.alpha": 0.5,
  "macrobase.analysis.mcd.stoppingDelta": 0.001,
  
  "macrobase.analysis.contextual.denseContextTau": 0.1,
  "macrobase.analysis.contextual.numIntervals": 10,
  
}


def process_config_parameters(config_parameters):
    for config_parameter_type in config_parameters:
        if type(config_parameters[config_parameter_type]) == list:
            config_parameters[config_parameter_type] = ", ".join(
                [str(para) for para in config_parameters[config_parameter_type]])


def create_config_file(config_parameters, conf_file):
    template_conf_file = batch_template_conf_file \
        if config_parameters["isBatchJob"] else streaming_template_conf_file
    template_conf_contents = open(template_conf_file, 'r').read()
    conf_contents = template_conf_contents % config_parameters
    with open(conf_file, 'w') as f:
        f.write(conf_contents)


def parse_results(results_file):
    times = dict()
    num_itemsets = 0
    num_iterations = 0
    tuples_per_second = 0.0
    tuples_per_second_no_itemset_mining = 0.0
    itemsets = list()
    with open(results_file, 'r') as f:
        lines = f.read().split('\n')
        for i in xrange(len(lines)):
            line = lines[i]
            if line.startswith("DEBUG"):
                if "time" in line and "...ended" in line:
                    line = line.split("...ended")[1].strip()
                    line_tokens = line.split("(")
                    time_type = line_tokens[0].strip()
                    time = int(line_tokens[1][6:-4])
                    times[time_type] = time
                elif "itemsets" in line:
                    line = line.split("Number of itemsets:")[1].strip()
                    num_itemsets = int(line)
                elif "iterations" in line:
                    line = line.split(
                        "Number of iterations in MCD step:")[1].strip()
                    num_iterations = int(line)
                elif "Tuples / second w/o itemset mining" in line:
                    line = line.split("Tuples / second w/o itemset mining = ")[1]
                    tuples_per_second_no_itemset_mining = float(
                        line.split("tuples / second")[0].strip())
                elif "Tuples / second" in line:
                    line = line.split("Tuples / second = ")[1]
                    tuples_per_second = float(
                        line.split("tuples / second")[0].strip())
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
    return (times, num_itemsets, num_iterations, itemsets, tuples_per_second,
            tuples_per_second_no_itemset_mining)

def separate_contextual_results(results_file):
    contextual_results_file = results_file + "_contextual"
    
    g = open(contextual_results_file,'w')
    
    with open(results_file, 'r') as f:
        lines = f.read().split('\n')
        for i in xrange(len(lines)):
            line = lines[i]
            if 'macrobase.analysis.BatchAnalyzer' in line or 'macrobase.analysis.contextualoutlier' in line:
                 g.write(line + '\n')
    g.close()

def get_stats(value_list):
    value_list = [float(value) for value in value_list]
    mean = sum(value_list) / len(value_list)
    stddev = (sum([(value - mean)**2 for value in value_list]) /
              len(value_list)) ** 0.5
    return mean, stddev


def run_workload(config_parameters, number_of_runs, print_itemsets=True):
    sub_dir = os.path.join(os.getcwd(),
                           testing_dir,
                           config_parameters["macrobase.query.name"],
                           strftime("%m-%d-%H:%M:%S"))
    os.system("mkdir -p %s" % sub_dir)
    # For now, we only run metrics with MCD and MAD: MCD for
    # dimensionalities greater than 1, MAD otherwise.
    dim = (len(config_parameters["macrobase.loader.targetHighMetrics"]) +
           len(config_parameters["macrobase.loader.targetLowMetrics"]))
    if dim > 1:
        config_parameters["macrobase.analysis.detectorType"] = "MCD"
    else:
        config_parameters["macrobase.analysis.detectorType"] = "MAD"
    process_config_parameters(config_parameters)
    conf_file = "batch.conf" if config_parameters["isBatchJob"] \
        else "streaming.conf"
    conf_file = os.path.join(sub_dir, conf_file)
    
    #create 
    config_parameters["macrobase.analysis.contextual.outputFile"] = os.path.join(sub_dir, "output.txt")
    
    create_config_file(config_parameters, conf_file)
    cmd = "pipeline" if config_parameters["isBatchJob"] else "streaming"

    all_times = dict()
    all_num_itemsets = list()
    all_num_iterations = list()
    all_itemsets = set()
    all_tuples_per_second = list()
    all_tuples_per_second_no_itemset_mining = list()

    for i in xrange(number_of_runs):
        results_file = os.path.join(sub_dir, "results%d.txt" % i)
        macrobase_cmd = '''java ${{JAVA_OPTS}} \\
            -cp "src/main/resources/:target/classes:target/lib/*:target/dependency/*" \\
            macrobase.MacroBase {cmd} {conf_file} \\
            > {results_file} 2>&1'''.format(
            cmd=cmd, conf_file=conf_file, results_file=results_file)
        print 'running the following command:'
        print macrobase_cmd
        os.system("cd ..; cd ..; %s" % macrobase_cmd)
        
        separate_contextual_results(results_file)
        #for inspecting contextual outlier performance
        
        
        (times, num_itemsets, num_iterations, itemsets,
            tuples_per_second, tuples_per_second_no_itemset_mining) = parse_results(results_file)

        for time_type in times:
            if time_type not in all_times:
                all_times[time_type] = list()
            all_times[time_type].append(times[time_type])

        all_num_itemsets.append(num_itemsets)
        all_num_iterations.append(num_iterations)
        for itemset in itemsets:
            all_itemsets.add(frozenset(itemset.items()))
        all_tuples_per_second.append(tuples_per_second)
        all_tuples_per_second_no_itemset_mining.append(tuples_per_second_no_itemset_mining)

    mean_and_stddev_times = dict()
    for time_type in all_times:
        mean_and_stddev_times[time_type] = get_stats(all_times[time_type])
    mean_num_itemsets, stddev_num_itemsets = get_stats(all_num_itemsets)
    mean_num_iterations, stddev_num_iterations = get_stats(all_num_iterations)
    mean_tuples_per_second, stddev_tuples_per_second = \
        get_stats(all_tuples_per_second)
    mean_tps_no_itemset_mining, stddev_tps_no_itemset_mining = \
        get_stats(all_tuples_per_second_no_itemset_mining)

    print config_parameters["macrobase.query.name"], "-->"
    print "Times:", mean_and_stddev_times

    print "Mean number of itemsets:", mean_num_itemsets,
    print ", Stddev number of itemsets:", stddev_num_itemsets

    print "Mean number of iterations:", mean_num_iterations,
    print ", Stddev number of iterations:", stddev_num_iterations

    if print_itemsets:
        print "Union of all itemsets:", list(all_itemsets)

    print "Mean tuples / second:", mean_tuples_per_second,
    print ", Stddev tuples / second:", stddev_tuples_per_second

    print "Mean tuples / second w/o itemset mining:", mean_tps_no_itemset_mining,
    print ", Stddev tuples / second w/o itemset mining:", stddev_tps_no_itemset_mining


def run_all_workloads(configurations, defaults, number_of_runs,
                      sweeping_parameter_name=None,
                      sweeping_parameter_value=None):
    if sweeping_parameter_name is not None:
        print "Running all workloads with",
        print sweeping_parameter_name, "=", sweeping_parameter_value
    else:
        print "Running all workloads with defaultParameters"
    print

    for config_parameters_raw in configurations:
        config_parameters = defaults.copy()
        config_parameters.update(config_parameters_raw)
        if sweeping_parameter_name is not None:
            config_parameters[sweeping_parameter_name] = \
                sweeping_parameter_value

        run_workload(config_parameters, number_of_runs)
        print


def _add_camel_case_argument(parser, argument, **kwargs):
    """
    Add an argument with a destination attribute being camel cased version
    of the argument instead of underscore separated version.
    """
    camel_case_str = ''.join([substr.title() if index != 0 else substr
                             for index, substr in enumerate(
                                argument.strip('-').split('-'))])
    parser.add_argument(argument, dest=camel_case_str, **kwargs)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--workflow-config',
                        type=argparse.FileType('r'),
                        default='contextual_workflow_config.json',
                        help='File with a list of parameters for workflow')
    parser.add_argument('--sweeping-parameters-config',
                        type=argparse.FileType('r'),
                        default='contextual_sweeping_parameters_config.json',
                        help='File with a dictionary of parameters to sweep')
    parser.add_argument('--number-of-runs', default=1, type=int,
                        help='Number of times to run a workload with same '
                             'parameters.')
    parser.add_argument('--compile', action='store_true',
                        help='Run mvn compile before running java code.')
    _add_camel_case_argument(parser, '--db-user')
    _add_camel_case_argument(parser, '--db-password')
    args = parser.parse_args()
    args.workflows = json.load(args.workflow_config)
    args.sweeping_parameters = json.load(args.sweeping_parameters_config)
    return args


if __name__ == '__main__':
    args = parse_args()

    if args.compile:
        status = os.system('cd .. && cd .. && mvn compile')
        if status != 0:
            os._exit(status)

    # Update default values if script argument is provided
    defaults = default_args.copy()
    for key, default_override in vars(args).items():
        if default_override is not None and key in defaults:
            defaults[key] = default_override

    run_all_workloads(args.workflows, defaults, args.number_of_runs)
    '''
    for parameter_name, values in args.sweeping_parameters.iteritems():
        for parameter_value in values:
            run_all_workloads(args.workflows, defaults,
                              args.number_of_runs,
                              sweeping_parameter_name=parameter_name,
                              sweeping_parameter_value=parameter_value)
    '''
