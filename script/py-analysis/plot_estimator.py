import argparse
import json
import matplotlib.pyplot as plt
import pandas as pd
import psycopg2


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
  parser.add_argument('--table', default='car_data_demo')
  parser.add_argument('--columns', nargs=1)
  parser.add_argument('--histogram-bins', default=10000, type=int)
  parser.add_argument('--estimates', type=argparse.FileType('r'),
                      help='File with inliers & outliers with their scores '
                           'outputted by macrobase')
  parser.add_argument('--x-limits', nargs=2, type=int, default=[-50, 250])
  add_db_args(parser)
  args = parser.parse_args()
  set_db_connection(args)
  return args


def format_datum(datum_with_score):
  """
  returns X, Y tuple, where X is data and Y is the score
  """
  data = datum_with_score['datum']['metrics']['data']
  if type(data) is list and len(data) == 1:
    data = data[0]
  return [data, datum_with_score['score']]


if __name__ == '__main__':
  args = parse_args()
  if args.estimates:
    estimates = json.load(args.estimates)
    outliers = [format_datum(datum) for datum in estimates['outliers']]
    inliers = [format_datum(datum) for datum in estimates['inliers']]
    all_data = outliers + inliers
    X, Y = zip(*sorted(all_data, key=lambda datum: datum[0]))
    scaling_factor = 1. * len(X) / args.histogram_bins
    scaledY = [scaling_factor * y for y in Y]

    (n, bins, patches) = plt.hist(X, args.histogram_bins)
    # plt.scatter(X, scaledY)
    plt.plot(X, scaledY)
    plt.show()
  # plt.plot(X, Y)
  # plt.xlim(args.x_limits)
  import os; os._exit(0)
  cursor = args.db_connection.cursor()
  cursor.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
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
  # plt.plot(bins)
 #  if args.estimates:
 #    estimates = json.load(args.estimates)
 #    outliers = [format_datum(datum) for datum in estimates['outliers']]
 #    inliers = [format_datum(datum) for datum in estimates['inliers']]
 #    all_data = outliers + inliers
 #    X, Y = zip(*all_data)
 #  plt.plot(X, Y)
  plt.xlim(args.x_limits)
  plt.show()
