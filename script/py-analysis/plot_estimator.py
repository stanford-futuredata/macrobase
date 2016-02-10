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


def parse_args():
  parser = argparse.ArgumentParser()
  parser.add_argument('--table', default='car_data_demo')
  parser.add_argument('--columns', nargs=1)
  parser.add_argument('--histogram-bins', default=10000, type=int)
  parser.add_argument('--estimates', type=argparse.FileType('r'),
                      help='File with inliers & outliers with their scores '
                           'outputted by macrobase')
  add_plot_limit_args(parser)
  parser.add_argument('--hist2d', choices=['inliers', 'outliers'],
                      help='Plots 2d histogram of outliers or inliers')
  add_db_args(parser)
  args = parser.parse_args()
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


def _plot_hist2d(args):
  plt.hist2d(data[args.hist2d[0]],
             data[args.hist2d[1]],
             bins=args.histogram_bins,
             norm=LogNorm())
  plt.colorbar()
  plt.xlabel(args.hist2d[0])
  plt.ylabel(args.hist2d[1])


if __name__ == '__main__':
  args = parse_args()
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
    X, Y = zip(*sorted(itertools.chain(outliers, inliers),
                       key=lambda datum: datum[0]))

    # leading term is purely fiction.. it should be 1/bandwidth
    scaling_factor = -50. * (outliers.shape[0] + inliers.shape[0]) / args.histogram_bins
    scaledY = [scaling_factor * y for y in Y]

    plt.hist([inliers[:, 0], outliers[:, 0]], args.histogram_bins,
             histtype='bar',
             stacked=True,
             label=['inliers', 'outliers'],
             color=['blue', 'red'])
    # plt.scatter(X, scaledY)
    plt.plot(X, scaledY, color='magenta', label='est distribution', lw=1.1)
    plt.legend(loc='upper left')
    plt.show()
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
  plt.show()
