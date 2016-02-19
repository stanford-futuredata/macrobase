import argparse
import itertools
import json
import matplotlib.pyplot as plt
import os
from common import add_db_args
from common import add_plot_limit_args
from common import set_db_connection
from common import set_plot_limits
from matplotlib.colors import LogNorm


SAVEFIG_INFER_VALUE = 'INFER_SAVEFIG_FILENAME'


def parse_args(*argument_list):
  parser = argparse.ArgumentParser()
  parser.add_argument('--estimates', type=argparse.FileType('r'),
                      required=True,
                      help='File with inliers & outliers with their scores '
                           'outputted by macrobase')
  parser.add_argument('--histogram-bins', default=100, type=int)
  parser.add_argument('--restrict-to', choices=['inliers', 'outliers'],
                      help='Plots 2d histogram of outliers or inliers')
  parser.add_argument('--columns', nargs='+', default=['metrics.*'],
                      help='Data to include in the plot')
  parser.add_argument('--legend-loc', default='best')
  parser.add_argument('--no-scores', action='store_false', default=True,
                      dest='plot_scores')
  parser.add_argument('--do-not-stack-hist', action='store_false',
                      default=True, dest='stacked_hist')
  parser.add_argument('--savefig', nargs='?', const=SAVEFIG_INFER_VALUE)
  add_plot_limit_args(parser)
  add_db_args(parser)
  args = parser.parse_args(*argument_list)
  if args.estimates is None:
    set_db_connection(args)
  return args


def _format_datum(datum_with_score, columns):
  """
  columns is a list of selectors for the data
  """
  data = []
  for column in columns:
    klass, index = column.split('.')
    assert klass in ('metrics', 'auxiliaries')
    if index == '*':
      data.extend(datum_with_score['datum'][klass]['data'])
    else:
      index = int(index)
      data.append(datum_with_score['datum'][klass]['data'][index])
  data.append(datum_with_score['score'])
  return data


def _extract_data(raw_data, label, columns, x_limits, y_limits):
  data = (_format_datum(datum, columns)
          for datum in raw_data[label])
  if x_limits:
    data = (x for x in data if x[0] > x_limits[0] and x[0] < x_limits[1])
  if y_limits:
    data = (x for x in data if x[1] > y_limits[0] and x[1] < y_limits[1])
  return data


def plot_estimator(args):
  estimates = json.load(args.estimates)

  dimensions = len(_format_datum(estimates['inliers'][0], args.columns)) - 1

  y_limits = None if dimensions != 2 else args.y_limits

  inliers = _extract_data(estimates, 'inliers', args.columns,
                          args.x_limits, y_limits)
  outliers = _extract_data(estimates, 'outliers', args.columns,
                           args.x_limits, y_limits)
  all_data = itertools.chain(inliers, outliers)

  print 'plotting %d dimensional plot' % dimensions

  if dimensions == 1:
    if args.restrict_to:
      data = _extract_data(estimates, args.restrict_to, args.columns,
                           args.x_limits, None)
      X, _ = zip(*data)
      plt.hist(X, args.histogram_bins,
               label=args.restrict_to)
    else:
      inliers = list(inliers)
      outliers = list(outliers)
      X1, _ = zip(*inliers)
      X2, _ = zip(*outliers)
      n, bins, patches = plt.hist([X1, X2], args.histogram_bins,
                                  histtype='bar',
                                  stacked=args.stacked_hist,
                                  label=['inliers', 'outliers'],
                                  color=['blue', 'red'])
      bin_width = bins[1] - bins[0]
      print 'plotted a curve with bin width =', bin_width

    if args.plot_scores:
      # Plot the estimate
      combined_X, scores = zip(*sorted(itertools.chain(inliers, outliers)))
      scaling_factor = bin_width * (len(estimates['inliers']) +
                                    len(estimates['outliers']))
      sign = 1. if scores[0] > 0 else -1
      scaling_factor *= sign
      scaled_scores = [scaling_factor * y for y in scores]
      plt.plot(combined_X, scaled_scores,
               color='magenta', label='est distribution', lw=1.1)
  elif dimensions == 2:
    if args.restrict_to:
      data = _extract_data(estimates, args.restrict_to, args.columns,
                           args.x_limits, args.y_limits)
    else:
      data = all_data
    X, Y, _ = zip(*data)
    plt.hist2d(X, Y,
               bins=args.histogram_bins,
               norm=LogNorm())
    plt.colorbar()

  plt.legend(loc=args.legend_loc)

  set_plot_limits(plt, args)
  if args.savefig is not None:
    if args.savefig == SAVEFIG_INFER_VALUE:
      name = os.path.basename(args.estimates.name).rsplit('.')[0]
      filename = 'target/plots/{}.png'.format(name)
    else:
      filename = args.savefig
    modifiers = []
    if args.restrict_to:
      modifiers.append(args.restrict_to)
    if args.estimates:
      modifiers.append('estimates')
    if args.x_limits:
      modifiers.append('X=%d,%d' % tuple(args.x_limits))
    if args.y_limits:
      modifiers.append('Y=%d,%d' % tuple(args.y_limits))
    name, ext = filename.rsplit('.')
    new_filename = '{old_name}-{modifiers}.{ext}'.format(
        old_name=name, modifiers='-'.join(modifiers), ext=ext)
    print 'saving figure to - ', new_filename
    plt.savefig(new_filename, dpi=320)
    plt.clf()
  else:
    plt.show()


if __name__ == '__main__':
  args = parse_args()
  plot_estimator(args)
