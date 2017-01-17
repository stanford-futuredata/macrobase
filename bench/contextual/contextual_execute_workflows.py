#!/usr/bin/python
import argparse
import json
import os
import platform
import subprocess, datetime, os, time, signal
from threading import Timer
from time import strftime


time_out = 12 * 60 * 60 # in seconds
max_heap = "-Xmx250g" # maximum memory allocation

testing_dir = "workflows"
batch_template_conf_file = "contextual_batch_template.conf"
streaming_template_conf_file = "contextual_streaming_template.conf"

default_args = {
  "macrobase.analysis.minOIRatio": 3.0,
  "macrobase.analysis.minSupport": 0.5,

  "macrobase.analysis.transformType": "MAD",
  "macrobase.loader.targetCategoricalMetrics": "",
  
  "macrobase.analysis.usePercentile": "true",
  "macrobase.analysis.targetPercentile": 0.99,
  "macrobase.analysis.useZScore": "false",
  "macrobase.loader.db.user": os.getenv('USER', None),
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
  
  "macrobase.loader.loaderType": "CACHING_POSTGRES_LOADER",
  
  "macrobase.analysis.contextual.denseContextTau": 0.1,
  "macrobase.analysis.contextual.numIntervals": 10,
  "macrobase.analysis.contextual.maxPredicates": 10000,
  "macrobase.analysis.classify.outlierStaticThreshold": 5.0,
  
  "macrobase.analysis.contextual.pruning.density": "true",
  "macrobase.analysis.contextual.pruning.triviality": "true",
  "macrobase.analysis.contextual.pruning.contextContainedInOutliers": "true",
  "macrobase.analysis.contextual.pruning.mad.noOutliers": "true",
  "macrobase.analysis.contextual.pruning.mad.containedOutliers": "true",
  
  "macrobase.analysis.contextual.api.suspiciousTuplesIndex": ""
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

def seperate_contextual_results(results_file):
    contextual_results_file = results_file + "_contextual"
    g = open(contextual_results_file,'w')
    
    contextStats = dict()
    
    with open(results_file, 'r') as f:
        lines = f.read().split('\n')
        for i in xrange(len(lines)):
            line = lines[i]
            if 'macrobase.analysis.BatchAnalyzer' in line or 'macrobase.analysis.contextualoutlier' in line or 'macrobase.analysis.pipeline.BasicContextualBatchedPipeline' in line:
                 g.write(line + '\n')
                 
            if "Done Contextual Outlier Detection Print Stats" in line:
                values = line.split(":")
                contextStats[values[len(values)-2]] = values[len(values)-1]
    g.close()
    
    return contextStats

def is_out_of_memory(results_file):
    with open(results_file, 'r') as f:
        lines = f.read().split('\n')
        for i in xrange(len(lines)):
            line = lines[i]
            if 'java.lang.OutOfMemoryError' in line:
                return True
    return False

def get_stats(value_list):
    value_list = [float(value) for value in value_list]
    mean = sum(value_list) / len(value_list)
    stddev = (sum([(value - mean)**2 for value in value_list]) /
              len(value_list)) ** 0.5
    return mean, stddev


def run_workload(config_parameters, number_of_runs, workflow_output_file, sweeping_parameter_config, print_itemsets=True):
    sub_dir = os.path.join(os.getcwd(),
                           testing_dir,
                           config_parameters["macrobase.query.name"],
                           strftime("%m-%d-%H:%M:%S"))
    os.system("mkdir -p %s" % sub_dir)
    # For now, we only run metrics with MCD and MAD: MCD for
    # dimensionalities greater than 1, MAD otherwise.
    dim = (len(config_parameters["macrobase.loader.targetHighMetrics"]) +
           len(config_parameters["macrobase.loader.targetLowMetrics"]))
    '''
    if dim > 1:
        config_parameters["macrobase.analysis.transformType"] = "MCD"
    else:
        config_parameters["macrobase.analysis.transformType"] = "MAD"
    '''
    process_config_parameters(config_parameters)
    conf_file = "batch.conf" if config_parameters["isBatchJob"] \
        else "streaming.conf"
    conf_file = os.path.join(sub_dir, conf_file)
    
    #create 
    config_parameters["macrobase.analysis.contextual.outputFile"] = os.path.join(sub_dir, "output.txt")
    
    create_config_file(config_parameters, conf_file)
    cmd = "pipeline" if config_parameters["isBatchJob"] else "streaming"

    
    '''
    all_times = dict()
    all_num_itemsets = list()
    all_num_iterations = list()
    all_itemsets = set()
    all_tuples_per_second = list()
    all_tuples_per_second_no_itemset_mining = list()
    '''
    all_contextStats = dict()
    
    
    for i in xrange(number_of_runs):
        results_file = os.path.join(sub_dir, "results%d.txt" % i)
        #different output file every time!
        config_parameters["macrobase.analysis.contextual.outputFile"] = os.path.join(sub_dir, "output%d.txt" % i)
        create_config_file(config_parameters, conf_file)
 
        macrobase_cmd = '''java  \\
            -Xms128m {max_heap} \\
            -cp "src/main/resources/:target/classes:target/lib/*:target/dependency/*" \\
            macrobase.MacroBase {cmd} {conf_file} \\
            > {results_file} 2>&1'''.format(max_heap = max_heap,
            cmd=cmd, conf_file=conf_file, results_file=results_file)
            
        if platform.node() == 'istc3':
            macrobase_cmd = '''java  \\
            -Dmacrobase.loader.db.url=localhost:5050 \\
            -Xms128m -Xmx128g \\
            -cp "src/main/resources/:target/classes:target/lib/*:target/dependency/*" \\
            macrobase.MacroBase {cmd} {conf_file} \\
            > {results_file} 2>&1'''.format(
            cmd=cmd, conf_file=conf_file, results_file=results_file)
            
        print 'running the following command:'
        print macrobase_cmd
        
        exit_status = "Completed"
     
        timeout_command_output = timeout_command("cd ..; cd ..; %s" % macrobase_cmd, time_out)
        if "Timeout" in timeout_command_output:
            exit_status = timeout_command_output
            
        if is_out_of_memory(results_file):
            exit_status = "OutOfMemory" + max_heap    
        #os.system("cd ..; cd ..; %s" % macrobase_cmd)
        
        print 'exit_status:'
        print exit_status
        
        contextStats = seperate_contextual_results(results_file)
        contextStats["exit_status"] = exit_status
        
        for key, value in contextStats.iteritems():
            if key not in all_contextStats.keys():
                all_contextStats[key] = list()
            all_contextStats[key].append(value)
        #for inspecting contextual outlier performance
        '''
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
        '''
    
    g = open(workflow_output_file,'a')   
    g.write("Query Name: " + config_parameters["macrobase.query.name"] + "\n")
    g.write("ResultDir: " + sub_dir + "\n");
    for key, value in sweeping_parameter_config.iteritems():
        g.write(str(key))
        g.write(": ")
        g.write(str(value))
        g.write("\n")
    for key, values in all_contextStats.iteritems():
        g.write(str(key))
        g.write(": ")
        #g.write(str(get_stats(values)[0]))
        g.write("|".join(values))
        g.write("\n")
    g.write("----------------------\n")
    g.close()
        
    '''
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
    '''

def run_all_workloads(configurations, defaults, number_of_runs, workflow_output_file,
                      sweeping_parameter_config):
    if sweeping_parameter_config:
        print "Running all workloads with",
        print sweeping_parameter_config
    else:
        print "Running all workloads with defaultParameters"
    print
    
    for config_parameters_raw in configurations:
        config_parameters = defaults.copy()
        config_parameters.update(config_parameters_raw)
        for sweeping_parameter_name, sweeping_parameter_value in sweeping_parameter_config.iteritems():
            config_parameters[sweeping_parameter_name] = \
                sweeping_parameter_value

        all_contextStats = run_workload(config_parameters, number_of_runs, workflow_output_file, sweeping_parameter_config)
        
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
    parser.add_argument('--workflow-output-file',
                        type=str,
                        default='workflow_output_file.txt',
                        help='Output the workflow stats to the file')
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

def timeout_command(command, timeout):
    """call shell-command and either return its output or kill it
          if it doesn't normally exit within timeout seconds and return None"""
    import subprocess, datetime, os, time, signal
    start = datetime.datetime.now()
    process = subprocess.Popen(command, shell=True, preexec_fn=os.setsid)
    while process.poll() is None:
        time.sleep(0.1)
        now = datetime.datetime.now()
        if (now - start).seconds > timeout:
          # os.kill(process.pid, signal.SIGKILL)
          os.killpg(os.getpgid(process.pid), signal.SIGKILL)
          # os.waitpid(-1, os.WNOHANG)
          return "Timeout %s" % timeout
    return "Completed Within %s" % timeout
  
def skip_cur_configuration(cur_configuration):
#     if cur_configuration["macrobase.analysis.contextual.pruning.distribution"] == False: 
#         if cur_configuration["macrobase.analysis.contextual.pruning.distribution.alpha"] == 1.0 and cur_configuration["macrobase.analysis.contextual.pruning.distribution.minsamplesize"] == 10000:
#             return False
#         else:
#             return True 
#     else:
#         return False
    return False

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

    '''
    run_all_workloads(args.workflows, defaults, args.number_of_runs)
    '''
            
            
    parameterNames = args.sweeping_parameters.keys()
    total_num_configurations = 1
    for parameter_name in parameterNames:
        print parameter_name, "has values ", len(args.sweeping_parameters[parameter_name])
        total_num_configurations *= len(args.sweeping_parameters[parameter_name])
    
    print "\n------------------------------\n"
    
    cur_configuration = { key: "" for key in parameterNames}
    for i in range(0, total_num_configurations):
        temp = i
        for parameter_name in parameterNames:
            parameter_value = args.sweeping_parameters[parameter_name][temp % len(args.sweeping_parameters[parameter_name])]
            temp = temp / len(args.sweeping_parameters[parameter_name])
            cur_configuration[parameter_name] = parameter_value
        print cur_configuration
        if skip_cur_configuration(cur_configuration):
            print "skipped"
            continue
        run_all_workloads(args.workflows, defaults,
                              args.number_of_runs,
                              args.workflow_output_file,
                              cur_configuration)
            
   
