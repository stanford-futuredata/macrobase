import matplotlib.pyplot as plt

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
        [times, itemsets] = line.split(" , ")
        split_point = times.find(": ")
        times = eval(times[split_point+2:])
        itemsets = int(itemsets.split(": ")[1])
        workload_name = lines[i-1].split("-->")[0].strip()
        try:
          [parameter_type, parameter_value] = parameters_description.split(" = ")
        except:
          continue
        parameter_value = float(parameter_value)
        if parameter_type not in parsed_results:
          parsed_results[parameter_type] = dict()
          parsed_results[parameter_type][workload_name] = dict()
        if workload_name not in parsed_results[parameter_type]:
          parsed_results[parameter_type][workload_name] = dict()
        parsed_results[parameter_type][workload_name][parameter_value] = (times, itemsets)
  return parsed_results

def plot_graphs(parsed_results):
  for parameter_type in parsed_results:
    for workload_name in parsed_results[parameter_type]:
      values = list()
      for parameter_value in sorted(parsed_results[parameter_type][workload_name].keys()):
        values.append(parameter_value)
      plt.plot(values)
      plt.show()

if __name__ == '__main__':
  parsed_results = parse_output_file("parameter_sweep.out")
  plot_graphs(parsed_results)
