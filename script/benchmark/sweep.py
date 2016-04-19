import argparse
import yaml
import os
from common import MacrobaseArgAction
from macrobase_cmd import run_macrobase
from time import strftime


def bench_directory(workload):
  sub_dir = os.path.join(
    os.getcwd(),
    'bench',
    'workflows',
    workload['macrobase.query.name'],
    strftime('%m-%d-%H:%M:%S'))
  os.system('mkdir -p %s' % sub_dir)
  return sub_dir


def parse_args():
  parser = argparse.ArgumentParser("""Example usage:

    python script/benchmark/run_sweeping.py bench/conf/treekde-test.yaml \\
      --macrobase-analysis-transformType MCD MAD KDE TREE_KDE
  """)
  parser.add_argument('experiment_yaml', type=argparse.FileType('r'))
  parser.add_argument('--macrobase-analysis-transformType',
                      nargs='*', const='sweeping_args',
                      action=MacrobaseArgAction)
  return parser.parse_args()


if __name__ == '__main__':
  args = parse_args()
  experiment = yaml.load(args.experiment_yaml)
  workload = experiment['macrobase']
  all_experiments = {}
  [(attr, values)] = args.sweeping_args.items()
  for value in values:
    kwargs = workload.copy()
    kwargs[attr] = value
    print kwargs
    _dir = bench_directory(kwargs)
    config_filename = os.path.join(_dir, 'config.yaml')
    with open(config_filename, 'w') as outfile:
      yaml.dump(kwargs, outfile)
    os.system('cat {}'.format(config_filename))
    run_macrobase(conf=config_filename)
