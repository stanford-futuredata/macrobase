"""
Plots a scatter plot of 2 metrics provided.
Data could be given from postgres or a csv file.
"""
import argparse
import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import psycopg2
import sys


def add_db_args(parser):
  parser.add_argument('--db-user', default='postgres')
  parser.add_argument('--db-name', default='postgres')
  parser.add_argument('--db-password')
  parser.add_argument('--db-host', default='localhost')
  parser.add_argument('--db-port', type=int)


def set_db_connection(args):
  def _parse_arg(**kwarg):
    [(key, value)] = kwarg.items()
    if value:
      return "{key}='{value}'".format(key=key, value=value)
    return ""

  args.db_connection = psycopg2.connect(" ".join([
    _parse_arg(dbname=args.db_name),
    _parse_arg(port=args.db_port),
    _parse_arg(user=args.db_user),
    _parse_arg(password=args.db_password),
    _parse_arg(host=args.db_host)]))


def parse_args():
  parser = argparse.ArgumentParser()

  source_group = parser.add_mutually_exclusive_group(required=True)
  source_group.add_argument('--csv', type=argparse.FileType('r'))
  source_group.add_argument('--table', default='car_data_demo')

  plot_type_group = parser.add_mutually_exclusive_group(required=True)
  plot_type_group.add_argument('--scatter', nargs=2)
  plot_type_group.add_argument('--histogram')

  parser.add_argument('--histogram-bins', type=int, default=100)
  parser.add_argument('--labels',
                      help='Labels for labeled data (different colors on the '
                           'plot)')
  parser.add_argument('--miscellaneous-cutoff', type=float, default=0.001,
                      help='Part of the data, that should a label have in order to be show in the plot')
  add_db_args(parser)
  args = parser.parse_args()
  if args.csv is None:
    set_db_connection(args)
  return args


if __name__ == '__main__':
  args = parse_args()
  if args.csv is None:
    cursor = args.db_connection.cursor()
    cursor.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")  # noqa
    print cursor.fetchall()
    sql = """
      SELECT {select} FROM {table};
    """.format(select='*', table=args.table)
    print sql
    colnames = [desc[0] for desc in cursor.description]
    data = pd.DataFrame(cursor.fetchall(), columns=colnames)
  else:
    data = pd.read_csv(args.csv)

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
      plt.scatter(data[args.scatter[0]], df[args.scatter[1]],
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
      plt.hist(data[args.histogram], args.histogram_bins,
               label=args.histogram)
    plt.xlabel(args.histogram)
    plt.ylim(ymax=int(data_size * args.miscellaneous_cutoff))

  plt.legend()
  plt.show()
