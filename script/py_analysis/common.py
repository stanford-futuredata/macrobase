import psycopg2
import re


def get_macrobase_camel_args(args):
  """
  Returns a dictionary of all database arguments set by
  add_db_args_camel_dest that were specified by the user.
  """
  allowed_args = {'dbUrl', 'dbUser', 'outlierDetectorType', 'dataTransform'}
  return {arg: value for arg, value in vars(args).items()
          if value is not None and arg in allowed_args}


def add_macrobase_args_dest_camel(parser):
  parser.add_argument('--db-url', dest='dbUrl')
  parser.add_argument('--db-user', dest='dbUser')
  parser.add_argument('--outlier-detector-type', dest='outlierDetectorType')
  parser.add_argument('--data-transform', dest='dataTransform')


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


def add_plot_limit_args(parser):
  parser.add_argument('--x-limits', nargs=2, type=int)
  parser.add_argument('--y-limits', nargs=2, type=int)


def set_plot_limits(plt, args):
  if args.x_limits:
    plt.xlim(args.x_limits)
  if args.y_limits:
    plt.ylim(args.y_limits)
