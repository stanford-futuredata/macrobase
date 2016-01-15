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
        if parameters_description not in parsed_results:
          parsed_results[parameters_description] = dict()
        parsed_results[parameters_description][workload_name] = (times, itemsets)
  return parsed_results

if __name__ == '__main__':
  parsed_results = parse_output_file("parameter_sweep.out")
  for parameters_description in parsed_results:
    for workload_name in parsed_results[parameters_description]:
      print parameters_description, workload_name, parsed_results[parameters_description][workload_name]
    print
