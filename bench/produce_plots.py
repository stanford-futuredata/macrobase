import matplotlib.pyplot as plt

workloads_to_be_plotted = [
  "cmtDatasetSimple",
  "cmtDatasetComplex",
  "milanTelecomSimple",
  "milanTelecomComplex",
  "campaignExpendituresSimple",
  "campaignExpendituresComplex",
  "fedDisbursementsSimple",
  "fedDisbursementsComplex"
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
    for timing_type in timing_types:
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
            values.append(parsed_results[parameter_type][workload_name][parameter_value][0][timing_type.lower()])
            keys.append(parameter_value)
          except:
            continue
        handle, = plt.plot(keys, values, label=workload_name)
        handles.append(handle)
      lgd = plt.legend(handles=handles, loc=(0.18, -0.65))
      plt.xlabel(parameter_type)
      plt.ylabel(timing_type + " time (in milliseconds)")
      plt.savefig(parameter_type + "_" + timing_type + '.png', bbox_extra_artists=(lgd,), bbox_inches='tight')

if __name__ == '__main__':
  parsed_results = parse_output_file("parameter_sweep.out")
  plot_graphs(parsed_results)
