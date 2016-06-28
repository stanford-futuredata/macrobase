"""
Compare speeds of different algorithm for training and testing.
e.g.
  python script/experiment/train_test_accuracy.py run --data bench/conf/data/cmt-5d-100k.yaml --algorithms bench/conf/algo/
"""
from __future__ import print_function
import argparse
import csv
import numpy as np
import seaborn as sns
import json
import re
import pandas as pd
import yaml
import os
from macrobase_cmd import run_diagnostic
import matplotlib.pyplot as plt
from collections import defaultdict
from exp_common import exp_bench_dir
from exp_common import makedirs_p


def parse_args():
  parser = argparse.ArgumentParser()
  subparsers = parser.add_subparsers()
  stats_parser = subparsers.add_parser('stats')
  stats_parser.set_defaults(func=run_dump_stats)
  stats_parser.add_argument('experiment_dir')
  stats_parser.add_argument('--save-csv')
  plot_parser = subparsers.add_parser('plot')
  plot_parser.set_defaults(func=run_plot_extracted)
  plot_parser.add_argument('experiment_dir')
  plot_parser.add_argument('--savefig')
  extract_parser = subparsers.add_parser('extract')
  extract_parser.set_defaults(func=run_extract_train_score_speed)
  extract_parser.add_argument('experiment_dir')
  run_parser = subparsers.add_parser('run')
  run_parser.set_defaults(func=run_loglikelihood_evolution)
  run_parser.add_argument('--data', type=argparse.FileType('r'),
                          required=True)
  run_parser.add_argument('--algorithms', required=True)
  run_parser.add_argument('--repeat', type=int, default=10)
  single_parser = subparsers.add_parser('single')
  single_parser.set_defaults(func=run_single_experiment)
  single_parser.add_argument('--data', type=argparse.FileType('r'),
                             required=True)
  single_parser.add_argument('--algo', type=argparse.FileType('r'),
                             required=True)
  return parser.parse_args()


THIS_EXPERIMENT_YAML = '''
macrobase.analysis.stat.trainTestSplit: 0.9
macrobase.pipeline.class: macrobase.analysis.pipeline.MixtureModelPipeline
macrobase.analysis.usePercentile: True
macrobase.query.name: loglikelihood_evolution-{data}-{algo}
macrobase.analysis.minSupport: 10
macrobase.analysis.minOIRatio: 10.0
logging:
  level: DEBUG
  loggers:
    "macrobase": TRACE
'''


def _filename_without_ext(file_path):
  return os.path.basename(file_path).rsplit('.')[-2]


def extract_train_score_speed(exp_dir):
  train_times = defaultdict(list)
  score_times = defaultdict(list)
  for alg in os.listdir(exp_dir):
    if not os.path.isdir(os.path.join(exp_dir, alg)):
      print('not dir', alg)
      continue
    for _try in os.listdir(os.path.join(exp_dir, alg)):
      with open(os.path.join(exp_dir, alg, _try, 'log.txt')) as logfile:
        lines = [l for l in logfile if re.search('took', l)]
      for line in lines:
        train_ms = re.search('training took (\d+)ms', line)
        score_ms = re.search('scoring took (\d+)ms', line)
        if train_ms is not None:
          train_times[alg].append(int(train_ms.group(1)))
        if score_ms is not None:
          score_times[alg].append(int(score_ms.group(1)))
  with open(os.path.join(exp_dir, 'times.json'), 'w') as outfile:
    json.dump({'train_times': train_times,
               'score_times': score_times}, outfile)


def _run(workload, _dir):
  config_filename = os.path.join(_dir, 'config.yaml')
  log_filename = os.path.join(_dir, 'log.txt')
  with open(config_filename, 'w') as outfile:
    yaml.dump(workload, outfile)
    os.system('cat {}'.format(config_filename))
    run_diagnostic('loglike', config_filename, save_log=log_filename)
    os.system('grep tuples {}'.format(log_filename))


def plot_violines(data):
  sns.violinplot(y='algorithm', x='ms', hue='tt', data=data)


def run_dump_stats(args):
  extract_train_score_speed(args.experiment_dir)
  with open(os.path.join(args.experiment_dir, 'times.json')) as infile:
    _json = json.load(infile)

  csv_header = ['algorithm', 'tt', 'mean_ms']
  csv_rows = []
  for time_type, _dict in _json.items():
    for alg, _list in _dict.items():
      csv_rows.append([alg, time_type, 1000. * 1e5 / np.mean(_list)])
  if args.save_csv is None:
    print(', '.join(csv_header))
    for row in csv_rows:
      print('{}, {}, {:.1f} '.format(*row))
    return
  with open(args.save_csv, 'w') as outfile:
    writer = csv.writer(outfile)
    writer.writerow(csv_header)
    for row in csv_rows:
      writer.writerow(row)


def run_plot_extracted(args):
  extract_train_score_speed(args.experiment_dir)
  with open(os.path.join(args.experiment_dir, 'times.json')) as infile:
    _json = json.load(infile)
  what, algos, values = [], [], []
  for time_type, _dict in _json.items():
    for alg, _list in _dict.items():
      what.extend(len(_list) * [time_type])
      algos.extend(len(_list) * [alg])
      values.extend(_list)
  df = pd.DataFrame()
  df['tt'] = what
  df['algorithm'] = algos
  df['ms'] = values
  plot_violines(df)
  if args.savefig:
    plt.savefig(args.savefig, dpi=320)
  else:
    plt.show()


def run_extract_train_score_speed(args):
  extract_train_score_speed(args.experiment_dir)


def run_loglikelihood_evolution(args):
  algorithms = [os.path.join(args.algorithms, algo)
                for algo in os.listdir(args.algorithms)
                if algo.endswith('.yaml')]
  experiment = yaml.load(THIS_EXPERIMENT_YAML.format(
    data=_filename_without_ext(args.data.name),
    algo='sweeping'))
  experiment.update(yaml.load(args.data))
  exp_dir = exp_bench_dir(experiment)
  for algo in algorithms:
    workload = experiment.copy()
    with open(algo, 'r') as infile:
      workload.update(yaml.load(infile))
    for repeat in range(args.repeat):
      _dir = os.path.join(exp_dir,
                          _filename_without_ext(algo),
                          str(repeat))
      makedirs_p(_dir)
      _run(workload, _dir)
  extract_train_score_speed(exp_dir)


def run_single_experiment(args):
  experiment = yaml.load(THIS_EXPERIMENT_YAML.format(
    data=_filename_without_ext(args.data.name),
    algo=_filename_without_ext(args.algo.name)))
  print(experiment)
  experiment.update(yaml.load(args.data))
  experiment.update(yaml.load(args.algo))
  _run(experiment, exp_bench_dir(experiment))

  _dir = exp_bench_dir(experiment)
  log_filename = os.path.join(_dir, 'log.txt')
  os.system('grep pace {}'.format(log_filename))
  os.system('grep iteration {} | wc -l'.format(log_filename))

if __name__ == '__main__':
  args = parse_args()
  args.func(args)
