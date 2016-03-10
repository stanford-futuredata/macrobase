import argparse
import psycopg2
import re


class MacrobaseArgAction(argparse.Action):
  def __call__(self, parser, namespace, values, option_string=None):
    arg = re.sub('_', '.', self.dest)
    macrobase_args = getattr(namespace, 'macrobase_args', {})
    macrobase_args[arg] = values
    setattr(namespace, 'macrobase_args', macrobase_args)


def add_macrobase_args(parser):
  parser.add_argument('--macrobase-loader-db-url', action=MacrobaseArgAction)
  parser.add_argument('--macrobase-loader-db-user', action=MacrobaseArgAction)
  parser.add_argument('--macrobase-analysis-kde-bandwidthMultiplier',
                      type=float,
                      action=MacrobaseArgAction)
  parser.add_argument('--macrobase-analysis-detectorType',
                      action=MacrobaseArgAction)
  parser.add_argument('--macrobase-analysis-treeKde-accuracy', type=float,
                      action=MacrobaseArgAction)
  parser.add_argument('--macrobase-analysis-treeKde-leafCapacity', type=int,
                      action=MacrobaseArgAction)


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
  parser.add_argument('--y-limits', nargs=2, type=float)
  parser.add_argument('--xscale')
  parser.add_argument('--x-limits', nargs=2, type=float)
  parser.add_argument('--xmax', type=float)
  parser.add_argument('--xmin', type=float)
  parser.add_argument('--yscale')
  parser.add_argument('--xlabel')
  parser.add_argument('--ylabel')
  parser.add_argument('--ymin', type=float)
  parser.add_argument('--ymax', type=float)
  parser.add_argument('--title')


def set_plot_limits(plt, args):
  if args.xlabel:
    plt.xlabel(args.xlabel)
  if args.ylabel:
    plt.ylabel(args.ylabel)
  if args.xmin:
    plt.xlim(xmin=args.xmin)
  if args.xmax:
    plt.xlim(xman=args.xman)
  if args.ymin:
    plt.ylim(ymin=args.ymin)
  if args.ymax:
    plt.ylim(ymax=args.ymax)
  if args.xscale:
    plt.xscale(args.xscale)
  if args.yscale:
    plt.yscale(args.yscale)
  if args.x_limits:
    plt.xlim(args.x_limits)
  if args.y_limits:
    plt.ylim(args.y_limits)
  if args.title:
    plt.title(args.title)
