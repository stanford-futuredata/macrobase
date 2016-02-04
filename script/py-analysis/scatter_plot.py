"""
Plots a scatter plot of 2 metrics provided.
Data could be given from postgres or a csv file.
"""
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import argparse


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

  parser.add_argument('--metrics', nargs=2, required=True)
  add_db_args(parser)
  args = parser.parse_args()
  if args.csv is None:
    set_db_connection(args)
  return args


if __name__ == '__main__':
  args = parse_args()
  if args.csv is None:
    cursor = args.db_connection.cursor()
    cursor.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
    print cursor.fetchall()
    sql = """
      SELECT {select} FROM {table};
    """.format(select='*', table=args.table)
    print sql
    colnames = [desc[0] for desc in cursor.description]
    data = pd.DataFrame(cursor.fetchall(), columns=colnames)
  else:
    data = pd.read_csv(args.csv)
  plt.scatter(data[args.metrics[0]], data[args.metrics[1]])
  plt.show()
