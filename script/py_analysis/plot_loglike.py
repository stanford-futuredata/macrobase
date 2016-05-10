import argparse
import os
import re
import matplotlib.pyplot as plt


def parse_args(*argument_list):
  parser = argparse.ArgumentParser()
  # parser.add_argument('logfile')
  parser.add_argument('directory')
  return parser.parse_args(*argument_list)


def get_points(logfile):
  regex = re.compile('iteration (\d+) is ([-]\d+[.]\d+)')
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
    x, y = get_points(os.path.join(args.directory, model))
    print model, x, y
    plt.plot(x, y, label=sterilize(model))
  plt.xlabel('iterations')
  plt.ylabel('log liklihood')
  plt.legend(loc='bottom right')
  plt.show()
