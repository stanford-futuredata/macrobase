import os

testing_dir = "workflows"
template_conf_file = "batch_template.conf"
all_config_parameters = \
[
  {
    "taskName": "testTasks",
    "targetAttributes": ["userid"],
    "targetLowMetrics": ["data_count_minutes"],
    "targetHighMetrics": [],

    "baseQuery": "SELECT * FROM mapmatch_history H, sf_datasets D WHERE H.dataset_id = D.id LIMIT 100000;"
  },
  {
    "taskName": "testTasks2",
    "targetAttributes": ["userid"],
    "targetLowMetrics": ["data_count_minutes"],
    "targetHighMetrics": [],

    "baseQuery": "SELECT * FROM mapmatch_history H, sf_datasets D WHERE H.dataset_id = D.id LIMIT 1000;"
  },
]

def process_config_parameters(config_parameters):
  for config_parameter_type in config_parameters:
    if type(config_parameters[config_parameter_type]) == list:
      config_parameters[config_parameter_type] = ", ".join([str(para) for para in config_parameters[config_parameter_type]])

def create_config_file(config_parameters, conf_file):
  template_conf_contents = open(template_conf_file, 'r').read()
  conf_contents = template_conf_contents % config_parameters
  with open(conf_file, 'w') as f:
    f.write(conf_contents)

if __name__ == '__main__':
  for config_parameters in all_config_parameters:
    sub_dir = os.path.join(os.getcwd(), testing_dir, config_parameters["taskName"])
    os.system("mkdir -p %s" % sub_dir)
    process_config_parameters(config_parameters)
    conf_file = os.path.join(sub_dir, "batch.conf")
    create_config_file(config_parameters, conf_file)
    os.system("cd ..; java ${JAVA_OPTS} -cp \"src/main/resources/:target/classes:target/lib/*:target/dependency/*\" macrobase.MacroBase batch %s" % conf_file) 
