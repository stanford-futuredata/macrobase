import argparse
import os
import re
import datetime
import matplotlib.pyplot as plt


def parse_args(*argument_list):
  parser = argparse.ArgumentParser()
  # parser.add_argument('logfile')
  parser.add_argument('directory')
  return parser.parse_args(*argument_list)


t_regex = re.compile('(2016)-(\d+)-(\d+) (\d+)[:](\d+)[:](\d+),(\d+)')
start_regex = re.compile('macrobase.util.TrainTestSpliter: nextDouble()')
regex = re.compile('iteration (\d+) is ([-]\d+[.]\d+)')


def get_time(line):
  return datetime.datetime(*[int(x) for x in t_regex.search(line).groups()])


def get_points(logfile):
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


def sterilize(name):
  return name


if __name__ == '__main__':
  args = parse_args()
  for model in os.listdir(args.directory):
    x, y = get_points(os.path.join(args.directory, model))
    print model, x, y
    plt.plot(x, y, label=sterilize(model))
  plt.xlabel('time (seconds)')
  plt.ylabel('log liklihood')
  plt.legend(loc='bottom right')
  plt.show()
