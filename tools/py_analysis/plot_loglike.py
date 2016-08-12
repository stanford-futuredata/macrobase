"""
Given a directory with log likelihood logs, for each file plots log
likelihood movement based on iteration or time.
"""
import argparse
import os
import re
import datetime
import matplotlib.pyplot as plt


t_regex = re.compile('(2016)-(\d+)-(\d+) (\d+)[:](\d+)[:](\d+),(\d+)')
start_regex = re.compile('macrobase.util.TrainTestSpliter: nextDouble()')
regex = re.compile('iteration (\d+) is ([-]\d+[.]\d+)')


def parse_args(*argument_list):
  parser = argparse.ArgumentParser()
  parser.add_argument('directory',
                      help='Directory with macrobase log files that contain '
                           'log likelihood logs.')
  parser.add_argument('--x-axis', choices=['time', 'iteration'],
                      default='iteration',
                      help='Variable to plot on x axis')
  return parser.parse_args(*argument_list)


def get_time(line):
  return datetime.datetime(*[int(x) for x in t_regex.search(line).groups()])


def get_time_points(logfile):
  lls = []
  start_time = None
  with open(logfile, 'r') as infile:
    for line in infile:
      if not start_time and start_regex.search(line):
        start_time = get_time(line)
      pattern = regex.search(line)
      if pattern:
        t = (get_time(line) - start_time).total_seconds()
        ll = float(pattern.group(2))
        lls.append((t, ll))
  return zip(*lls)


def get_iter_points(logfile):
  lls = []
  with open(logfile, 'r') as infile:
    for line in infile:
      pattern = regex.search(line)
      if pattern:
        i = int(pattern.group(1))
        ll = float(pattern.group(2))
        lls.append((i, ll))
  return zip(*lls)


def sterilize(name):
  return name


if __name__ == '__main__':
  args = parse_args()
  for model in os.listdir(args.directory):
    get_points = get_time_points if args.x_axis == 'time' else get_iter_points
    x, y = get_points(os.path.join(args.directory, model))
    print model, x, y
    plt.plot(x, y, label=sterilize(model))
  plt.xlabel(args.x_axis)
  plt.ylabel('log liklihood')
  plt.legend(loc='bottom right')
  plt.show()
