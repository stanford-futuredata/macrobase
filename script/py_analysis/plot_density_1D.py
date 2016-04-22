import json
import matplotlib.pyplot as plt
import numpy as np
import argparse


def parse_args(*argument_list):
  parser = argparse.ArgumentParser()
  parser.add_argument('dump_density_grid_file',
      help=('File that was dumped using'  # noqa
            '"macrobase.diagnostic.dumpDensityGridFile" option in macrobase'))
  parser.add_argument('--dump-score-file', help='File that was dumped using'
      '"macrobase.diagnostic.dumpScoreFile" option in macrobase')  # noqa
  args = parser.parse_args(*argument_list)
  return args


def load_json_dump(filename):
  with open(filename) as infile:
    data = json.load(infile)
  return zip(*[(point['metrics']['data'], point['density'])
               for point in data])


if __name__ == '__main__':
  args = parse_args()
  X, Y = load_json_dump(args.dump_density_grid_file)

  XX, YY = load_json_dump(args.dump_score_file)
  _, bins, _ = plt.hist(XX)
  w = bins[1] - bins[0]
  Y = np.array(Y) * w * len(XX)
  plt.plot(X, Y)
  plt.show()
