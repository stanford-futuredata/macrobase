import argparse
import itertools
import json
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
from common import add_db_args
from common import add_plot_limit_args
from common import set_db_connection
from common import set_plot_limits
from matplotlib.colors import LogNorm
from plot_estimator import _format_datum
from plot_estimator import _extract_data


def parse_args(*argument_list):
  parser = argparse.ArgumentParser()
  parser.add_argument('infiles', type=argparse.FileType('r'), nargs='+',
                      help='File(s) with inliers & outliers with their scores '
                           'outputted by macrobase')
  parser.add_argument('--histogram-bins', default=100, type=int)
  parser.add_argument('--restrict-to', choices=['inliers', 'outliers'],
                      help='Plots 2d histogram of outliers or inliers')
  parser.add_argument('--columns', nargs=1, default=['metrics.*'],
		      help='Data to include in the plot')
  parser.add_argument('--legend-loc', default='best')
  parser.add_argument('--no-scores', action='store_false', default=True, dest='plot_scores')
  parser.add_argument('--savefig')
  add_plot_limit_args(parser)
  add_db_args(parser)
  args = parser.parse_args(*argument_list)
  return args


def _format_data(infile, args):
  print 'formatting data from file %s' % infile.name
  raw_data = json.load(infile)
  dimensions = len(_format_datum(raw_data['inliers'][0], args.columns)) - 1
  assert dimensions == 1
  outliers = _extract_data(raw_data, 'outliers', args.columns, args.x_limits, None)
  return os.path.basename(infile.name).rsplit('.')[0], list(outliers)

def plot_histograms(args):
  classifiers = {}
  data, labels = [], []
  for _file in args.infiles:
    label, content = _format_data(_file, args)
    labels.append(label)
    X, _ = zip(*content)
    data.append(X)
  
  plt.hist(data, args.histogram_bins, histtype='bar', stacked=False, label=labels)

  plt.legend(loc=args.legend_loc)

  set_plot_limits(plt, args)
  if args.savefig is not None:
    filename = args.savefig
    modifiers = []
    if args.x_limits:
      modifiers.append('X=%d,%d' % tuple(args.x_limits))
    if args.y_limits:
      modifiers.append('Y=%d,%d' % tuple(args.y_limits))
    name, ext = filename.rsplit('.')
    new_filename = '{old_name}-{modifiers}.{ext}'.format(old_name=name, modifiers='-'.join(modifiers), ext=ext)
    print 'saving figure to - ', new_filename
    plt.savefig(new_filename, dpi=320)
    plt.clf()
  else:
    plt.show()


if __name__ == '__main__':
  args = parse_args()
  plot_histograms(args)
