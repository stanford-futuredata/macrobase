"""
Plots a scatter plot of 2 metrics provided.
Data could be given from postgres or a csv file.
"""
from matplotlib.colors import LogNorm
from mpl_toolkits.mplot3d import Axes3D
import sys
import numpy as np
import argparse
import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
from common import add_db_args
from common import add_plot_limit_args
from common import set_db_connection
from common import set_plot_limits


def parse_args(*argument_list):
  parser = argparse.ArgumentParser()

  source_group = parser.add_mutually_exclusive_group(required=True)
  source_group.add_argument('--csv')
  source_group.add_argument('--table')
  source_group.add_argument('--query')

  plot_type_group = parser.add_mutually_exclusive_group(required=True)
  plot_type_group.add_argument('--scatter', nargs=2)
  plot_type_group.add_argument('--histogram')
  plot_type_group.add_argument('--hist2d', nargs=2)
  plot_type_group.add_argument('--scatter3d', nargs=3)

  parser.add_argument('--histogram-bins', type=int, default=100)
  parser.add_argument('--filter-num-rtus', type=int)
  parser.add_argument('--filter-controller', type=int)
  parser.add_argument('--labels',
                      help='Labels for labeled data (different colors on the '
                           'plot)')
  parser.add_argument('--miscellaneous-cutoff', type=float, default=0.001,
                      help='Part of the data, that should a label have in '
                           'order to be show in the plot')
  parser.add_argument('--do-not-scale-down', action='store_false',
                      dest='scale_down')
  parser.add_argument('--scale-down', action='store_true')
  parser.add_argument('--savefig')
  add_plot_limit_args(parser)
  add_db_args(parser)
  args = parser.parse_args(*argument_list)
  if args.csv is None:
    set_db_connection(args)
  return args


def plot_scatter3d(data, args):
  data = data[data[args.scatter3d[0]].notnull()][data[args.scatter3d[1]].notnull()][data[args.scatter3d[2]].notnull()]
  data = data[:100000]
  print len(data)
  fig = plt.figure()
  ax = fig.add_subplot(111, projection='3d')
  ax.scatter(data[args.scatter3d[0]],
             data[args.scatter3d[1]],
             data[args.scatter3d[2]])


def _plot_hist2d(data, args):
  data = data[data[args.hist2d[0]].notnull()][data[args.hist2d[1]].notnull()]
  if data.shape[0] < 1000:
    sys.exit(1)
  df = data.replace([np.inf, -np.inf], np.nan).dropna(subset=args.hist2d)
  plt.hist2d(df[args.hist2d[0]].astype(float),
             df[args.hist2d[1]].astype(float),
             bins=args.histogram_bins,
             norm=LogNorm())
  plt.colorbar()
  set_plot_limits(plt, args)
  plt.xlabel(args.hist2d[0])
  plt.ylabel(args.hist2d[1])
  set_plot_limits(plt, args)
  plt.title("N = {}".format(data.shape[0]))


def plot_distribution(args):
  if args.csv is not None:
    data = pd.read_csv(args.csv)
    print ' '.join(list(data.columns.values))
    if args.filter_num_rtus:
      print 'before filtering size =', data.shape[0]
      data = data[data['num_rtus'] == args.filter_num_rtus]
      print 'after filtering size =', data.shape[0]
    if args.filter_controller:
      print 'before filtering size =', data.shape[0]
      data = data[data['controller_id'] == args.filter_controller]
      print 'after filtering size =', data.shape[0]
    if 'controller_id' in data:
      print 'total controller_ids included =', len(set(data['controller_id']))
    if 'num_rtus' in data:
      print 'distinct num_rtus =', len(set(data['num_rtus'])), set(data['num_rtus'])
  else:
    cursor = args.db_connection.cursor()
    cursor.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")  # noqa
    if args.query:
      with open(args.query, 'r') as infile:
        sql = ''.join(list(infile))
    else:
      sql = """
        SELECT {select} FROM {table};
      """.format(select='*', table=args.table)
    print sql
    cursor.execute(sql)
    colnames = [desc[0] for desc in cursor.description]
    data = pd.DataFrame(cursor.fetchall(), columns=colnames)

  # Set args.data, so we can pass only args to functions
  args.data = data

  data_size = data.shape[0]

  if args.scatter is not None:
    if args.labels:
      interesting_data = data[[args.scatter[0], args.scatter[1], args.labels]]
      different_labels = set(data[args.labels])
      for label, color in zip(different_labels,
                              matplotlib.colors.cnames.keys()):
        df = interesting_data.query('{column} == "{label}"'.format(
                                    column=args.labels, label=label))
        plt.scatter(df[args.scatter[0]], df[args.scatter[1]],
                    c=color, label=label)
    else:
      plt.scatter(data[args.scatter[0]], data[args.scatter[1]],
                  c=color)
    plt.xlabel(args.scatter[0])
    plt.ylabel(args.scatter[1])
  elif args.histogram is not None:
    if args.labels:
      interesting_data = data[[args.histogram, args.labels]]
      different_labels = set(data[args.labels])
      data_to_plot, colors_to_use, labels_to_show = [], [], []
      miscellaneous_labels = set()
      misc_frame, misc_color = pd.DataFrame(), None
      for label, color in zip(different_labels,
                              matplotlib.colors.cnames.keys()):
        df = interesting_data.query('{column} == "{label}"'.format(
                                    column=args.labels, label=label))
        if df.shape[0] < args.miscellaneous_cutoff * data_size:
          miscellaneous_labels.add(label)
          misc_frame = pd.concat([misc_frame, df[args.histogram]])
          misc_color = color
          continue
        labels_to_show.append('{label} ({count})'.format(label=label,
                                                         count=df.shape[0]))
        data_to_plot.append(df[args.histogram])
        colors_to_use.append(color)
      if misc_color is not None:
        labels_to_show.append('miscellaneous ({count})'.format(
                              count=misc_frame.shape[0]))
        data_to_plot.append(misc_frame)
        # colors_to_use.append(misc_color)
        colors_to_use.append('cyan')
      plt.hist(data_to_plot, args.histogram_bins, histtype='bar',
               color=colors_to_use, label=labels_to_show)
    else:
      df = data.replace([np.inf, -np.inf], np.nan).dropna(subset=[args.histogram])
      plt.hist(df[args.histogram].astype(float),
               bins=args.histogram_bins,
               label=args.histogram)
      plt.yscale('log')

    plt.xlabel(args.histogram)
    if args.scale_down:
      plt.ylim(ymax=int(data_size * args.miscellaneous_cutoff))
  elif args.hist2d is not None:
    _plot_hist2d(data, args)
  elif args.scatter3d is not None:
    plot_scatter3d(data, args)

  plt.legend()
  if not args.scatter3d and not args.histogram:
    set_plot_limits(plt, args)
  if args.savefig is not None:
    plt.savefig(args.savefig, dpi=320)
    plt.clf()
  else:
    plt.show()

if __name__ == '__main__':
  args = parse_args()
  plot_distribution(args)
