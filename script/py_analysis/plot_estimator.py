import argparse
import json
import itertools
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from common import add_db_args
from common import add_plot_limit_args
from common import set_db_connection
from common import set_plot_limits
from matplotlib.colors import LogNorm


def parse_args(*argument_list):
  parser = argparse.ArgumentParser()
  parser.add_argument('--table', default='car_data_demo')
  parser.add_argument('--columns', nargs=1)
  parser.add_argument('--histogram-bins', default=100, type=int)
  parser.add_argument('--estimates', type=argparse.FileType('r'),
                      help='File with inliers & outliers with their scores '
                           'outputted by macrobase')
  parser.add_argument('--hist2d', choices=['inliers', 'outliers'],
                      help='Plots 2d histogram of outliers or inliers')
  parser.add_argument('--legend-loc', default='best')
  parser.add_argument('--savefig')
  add_plot_limit_args(parser)
  add_db_args(parser)
  args = parser.parse_args(*argument_list)
  if args.estimates is None:
    set_db_connection(args)
  return args


def format_datum_1d(datum_with_score):
  """
  returns X, Y tuple, where X is data and Y is the score
  """
  data = datum_with_score['datum']['metrics']['data']
  if type(data) is list and len(data) == 1:
    data = data[0]
  return [data, datum_with_score['score']]


def format_datum(datum_with_score):
  """
  Returns scores appended to data points
  """
  data = datum_with_score['datum']['metrics']['data']
  return data + [datum_with_score['score']]


def _only_within(array, lower_limit, upper_limit):
  return [x for x in array if x > lower_limit and x < upper_limit]

def plot_estimator(args):
  if args.hist2d:
    estimates = json.load(args.estimates)
    data = np.array([format_datum(datum)
                     for datum in estimates['outliers']])
    plt.hist2d(data[:, 0],
               data[:, 1],
               bins=args.histogram_bins,
               norm=LogNorm())
    plt.colorbar()
  elif args.estimates:
    estimates = json.load(args.estimates)
    outliers = np.array([format_datum_1d(datum)
                         for datum in estimates['outliers']])
    inliers = np.array([format_datum_1d(datum) for datum in estimates['inliers']])
    combined_data = sorted(itertools.chain(outliers, inliers),
                       	   key=lambda datum: datum[0])
    print len(combined_data)
    if args.x_limits:
      data_in_limits = [point for point in combined_data
                        if point[0] > args.x_limits[0] and point[0] < args.x_limits[1]]
      print len(data_in_limits)
    else:
      data_in_limits = combined_data
    X, Y = zip(*data_in_limits)

    # leading term is purely fiction.. it should be 1/bandwidth
    scaling_factor = 50. * (outliers.shape[0] + inliers.shape[0]) / args.histogram_bins
    sign = 1. if Y[0] > 0 else -1
    scaling_factor *= sign
    scaledY = [scaling_factor * y for y in Y]

    if args.x_limits:
      inliers_to_plot = _only_within(inliers[:, 0], args.x_limits[0], args.x_limits[1])
      outliers_to_plot = _only_within(outliers[:, 0], args.x_limits[0], args.x_limits[1])
    else:
      inliers_to_plot, outliers_to_plot = inliers[:, 0], outliers[:, 0]
    plt.hist([inliers_to_plot, outliers_to_plot], args.histogram_bins,
             histtype='bar',
             stacked=True,
             label=['inliers', 'outliers'],
             color=['blue', 'red'])
    # plt.scatter(X, scaledY)
    plt.plot(X, scaledY, color='magenta', label='est distribution', lw=1.1)
    plt.legend(loc=args.legend_loc)
  else:
    cursor = args.db_connection.cursor()
    cursor.execute("select relname from pg_class "
                   "where relkind='r' and relname !~ '^(pg_|sql_)';")
    print cursor.fetchall()
    sql = """
      SELECT {select} FROM {table};
    """.format(select='*', table=args.table)
    print sql
    cursor.execute(sql)
    colnames = [desc[0] for desc in cursor.description]
    print colnames
    data = pd.DataFrame(cursor.fetchall(), columns=colnames)
    print [float(x) for x in data[args.columns[0]][:20]]
    (n, bins, patches) = plt.hist([float(x) for x in data[args.columns[0]]],
                                  args.histogram_bins)
  set_plot_limits(plt, args)
  if args.savefig is not None:
    filename = args.savefig
    modifiers = []
    if args.hist2d:
      modifiers.append('hist2d')
    if args.estimates:
      modifiers.append('estimates')
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
  plot_estimator(args)
