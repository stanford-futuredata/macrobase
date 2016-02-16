"""
Generates a multimodal distribution with some outliers
"""
import argparse
import csv
import numpy as np


class DistributionAction(argparse.Action):
  def __call__(self, parser, namespace, values, option_string=None):
    current_value = getattr(namespace, self.dest) or []
    distribution_type = self.option_strings[0].lstrip('-    ')
    if type(values) is not list:
      values = [values]
    for values_string in values:
      distribution_attrs = [float(x) for x in values_string.split(',')]
      if not isinstance(self.const, (list, tuple)):
        self.const = [self.const]
      assert len(distribution_attrs) in self.const
      current_value.append(tuple([distribution_type] + distribution_attrs))
    setattr(namespace, self.dest, current_value)


def parse_args(*argument_array):
  parser = argparse.ArgumentParser()
  subparsers = parser.add_subparsers(title='Dimensionality')

  oneD = subparsers.add_parser('1D', help='1 dimensional distribution')
  oneD.add_argument('--normal', nargs='*', action=DistributionAction,
                    dest='distributions', const=3)
  oneD.add_argument('--laplace', nargs='*', action=DistributionAction,
                    dest='distributions', const=3)
  oneD.add_argument('--uniform', nargs='*', action=DistributionAction,
                    dest='distributions', const=3)
  oneD.add_argument('--outfile', type=argparse.FileType('w'))
  oneD.set_defaults(dimensions=1)

  twoD = subparsers.add_parser('2D', help='2 dimensional distribution')
  twoD.add_argument('--normal', nargs='*', action=DistributionAction,
                    dest='distributions', const=(4, 7))

  twoD.add_argument('--outfile', type=argparse.FileType('w'))
  twoD.set_defaults(dimensions=2)
  args = parser.parse_args(*argument_array)
  return args


def generate_distribution(args):
  header = []
  all_points = None
  if args.dimensions == 1:
    for name, mean, parameter, num_items in args.distributions:
      numpy_gen = getattr(np.random, name)
      numbers = numpy_gen(mean, parameter, num_items)
      if all_points is not None:
        all_points = np.append(all_points, numbers)
      else:
        all_points = numbers
    header.append('XX')
  elif args.dimensions == 2:
    header = ['XX', 'YY']
    for distribution_params in args.distributions:
      if distribution_params[0] == 'normal':
        mean = np.array(distribution_params[1:3])
        num_items = int(distribution_params[-1])
        if len(distribution_params) == 5:
          std = distribution_params[3]
          covariance_matrix = np.array([[std ** 2, 0], [0, std ** 2]])
        else:
          covariance_matrix = np.array([distribution_params[3:5],
                                        distribution_params[5:7]])
        gen_points = np.random.multivariate_normal(mean, covariance_matrix,
                                                   num_items)
        if all_points is None:
          all_points = gen_points
        else:
          all_points = np.concatenate([all_points, gen_points])
  writer = csv.writer(args.outfile)
  writer.writerow(header)
  for x in all_points:
    if args.dimensions == 1:
      writer.writerow([x])
    else:
      writer.writerow(x)

if __name__ == '__main__':
  args = parse_args()
  generate_distribution(args)
  args.outfile.close()
