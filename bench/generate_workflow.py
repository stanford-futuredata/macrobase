import os

testing_dir = "workflows"
batch_template_conf_file = "batch_template.conf"
streaming_template_conf_file = "streaming_template.conf"
all_config_parameters = \
[
  {
    "isBatchJob": True,
    "taskName": "testTasks",
    "targetAttributes": ["device_id", "state", "model", "firmware_version"],
    "targetLowMetrics": [],
    "targetHighMetrics": ["power_drain"],

    "baseQuery": "SELECT * FROM sensor_data;"
  },
  {
    "isBatchJob": False,
    "taskName": "testTasks2",
    "targetAttributes": ["device_id", "state", "model", "firmware_version"],
    "targetLowMetrics": [],
    "targetHighMetrics": ["power_drain"],

    "inputReservoirSize": 10000,
    "scoreReservoirSize": 10000,
    "inlierItemSummarySize": 1000,
    "outlierItemSummarySize": 10000,
    "summaryRefreshPeriod": 100000,
    "modelRefreshPeriod": 10000,
    "warmupCount": 1000,

    "baseQuery": "SELECT * FROM sensor_data;"
  }
]

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

if __name__ == '__main__':
  for config_parameters in all_config_parameters:
    sub_dir = os.path.join(os.getcwd(), testing_dir, config_parameters["taskName"])
    os.system("mkdir -p %s" % sub_dir)
    process_config_parameters(config_parameters)
    conf_file = "batch.conf" if config_parameters["isBatchJob"] else "streaming.conf"
    conf_file = os.path.join(sub_dir, conf_file)
    create_config_file(config_parameters, conf_file)
    cmd = "batch" if config_parameters["isBatchJob"] else "streaming"
    os.system("cd ..; java ${JAVA_OPTS} -cp \"src/main/resources/:target/classes:target/lib/*:target/dependency/*\" macrobase.MacroBase %s %s" % (cmd, conf_file))
