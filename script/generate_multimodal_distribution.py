"""
Generates a multimodal distribution with some outliers
"""
import argparse
import csv
import numpy as np


class DistributionAction(argparse.Action):
  def __call__(self, parser, namespace, values, option_string=None):
    current_value = getattr(namespace, self.dest) or []
    distribution_type = self.option_strings[0].lstrip('-')
    if type(values) is not list:
      values = [values]
    for values_string in values:
      distribution_attrs = [float(x) for x in values_string.split(',')]
      assert len(distribution_attrs) == 3
      current_value.append(tuple([distribution_type] + distribution_attrs))
    setattr(namespace, self.dest, current_value)


def parse_args():
  parser = argparse.ArgumentParser()
  parser.add_argument('--normal', nargs='*', action=DistributionAction,
                      dest='distributions')
  parser.add_argument('--laplace', nargs='*', action=DistributionAction,
                      dest='distributions')
  parser.add_argument('--uniform', nargs='*', action=DistributionAction,
                      dest='distributions')
  parser.add_argument('--outfile', type=argparse.FileType('w'))
  args = parser.parse_args()
  return args

if __name__ == '__main__':
  args = parse_args()
  all_points = []
  for name, mean, parameter, num_items in args.distributions:
    numpy_gen = getattr(np.random, name)
    numbers = numpy_gen(mean, parameter, num_items)
    all_points = np.append(all_points, numbers)
  writer = csv.writer(args.outfile)
  writer.writerow(['data'])
  for x in all_points:
    writer.writerow([x])
