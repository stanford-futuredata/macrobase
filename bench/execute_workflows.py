import os

from config_parameters import all_config_parameters

testing_dir = "workflows"
batch_template_conf_file = "batch_template.conf"
streaming_template_conf_file = "streaming_template.conf"

default_streaming_args = {
  "inputReservoirSize": 10000,
  "scoreReservoirSize": 10000,
  "inlierItemSummarySize": 1000,
  "outlierItemSummarySize": 10000,
  "summaryRefreshPeriod": 100000,
  "modelRefreshPeriod": 10000,
  "warmupCount": 1000
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
  with open(results_file, 'r') as f:
    lines = f.read().split('\n')
    for line in lines:
      if line.startswith("DEBUG"):
        if "time" in line:
          line = line.split("...ended")[1].strip()
          line_tokens = line.split()
          time_type = line_tokens[0]
          time = int(line_tokens[2][:-4])
          times[time_type] = time
        elif "itemsets" in line:
          line = line.split("Number of itemsets:")[1].strip()
          num_itemsets = int(line)
  return times, num_itemsets

if __name__ == '__main__':
  for config_parameters_raw in all_config_parameters:
    config_parameters = {}
    for key in default_streaming_args:
      config_parameters[key] = default_streaming_args[key]
    for key in config_parameters_raw:
      config_parameters[key] = config_parameters_raw[key]
    sub_dir = os.path.join(os.getcwd(), testing_dir, config_parameters["taskName"])
    os.system("mkdir -p %s" % sub_dir)
    process_config_parameters(config_parameters)
    conf_file = "batch.conf" if config_parameters["isBatchJob"] else "streaming.conf"
    conf_file = os.path.join(sub_dir, conf_file)
    results_file = os.path.join(sub_dir, "results.txt")
    create_config_file(config_parameters, conf_file)
    cmd = "batch" if config_parameters["isBatchJob"] else "streaming"
    os.system("cd ..; java ${JAVA_OPTS} -cp \"src/main/resources/:target/classes:target/lib/*:target/dependency/*\" macrobase.MacroBase %s %s > %s" % (cmd, conf_file, results_file))
    times, num_itemsets = parse_results(results_file)
    print config_parameters["taskName"], "-->"
    print "Times:", times, ", Number of itemsets:", num_itemsets
