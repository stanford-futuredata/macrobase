"""
Compare speeds of different algorithm for training and testing.
e.g.
  python script/experiment/train_test_accuracy.py run --data bench/conf/data/cmt-5d-100k.yaml --algorithms bench/conf/algo/
"""
from __future__ import print_function
import argparse
import csv
from datetime import datetime
import numpy as np
import seaborn as sns
import json
import re
import pandas as pd
import yaml
import os
from macrobase_cmd import run_macrobase
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
  plot_iter_parser = subparsers.add_parser('plot_loglike_over_iterations')
  plot_iter_parser.set_defaults(func=run_plot_loglike_over_iterations)
  plot_iter_parser.add_argument('experiment_dir')
  plot_iter_parser.add_argument('--savefig')
  plot_time_parser = subparsers.add_parser('plot_loglike_over_time')
  plot_time_parser.set_defaults(func=run_plot_loglike_over_time)
  plot_time_parser.add_argument('experiment_dir')
  plot_time_parser.add_argument('--savefig')
  plot_dist_parser = subparsers.add_parser('plot_iteration_distribution')
  plot_dist_parser.set_defaults(func=run_plot_iteration_distribution)
  plot_dist_parser.add_argument('experiment_dir')
  plot_dist_parser.add_argument('--savefig')
  extract_parser = subparsers.add_parser('extract')
  extract_parser.set_defaults(func=run_extract_statistics)
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
macrobase.pipeline.class: macrobase.analysis.pipeline.CarefulInitializationPipeline
macrobase.analysis.usePercentile: True
macrobase.query.name: convergence_consistency-{data}-{algo}
macrobase.analysis.minSupport: 10
macrobase.analysis.minOIRatio: 10.0
logging:
  level: DEBUG
  loggers:
    "macrobase": TRACE
'''


def _filename_without_ext(file_path):
  return os.path.basename(file_path).rsplit('.')[-2]


loglike_regex = re.compile('test loglike = ([-]?\d+[.]\d+)')
time_regex = re.compile('DEBUG \[(\d{4}[-]\d{2}[-]\d{2} \d{2}:\d{2}:\d{2},\d{3})\]')
time_format = '%Y-%m-%d %H:%M:%S,%f'
iteration_start_regex = re.compile(r'macrobase.analysis.stats.mixture.Variational')

def _time(logline):
  timestring = time_regex.search(logline).group(1) + '000'  # convert milliseconds to microseconds
  t = datetime.strptime(timestring, time_format)
  return t

def extract_logfile_stats(logfile):
  stats = {'iterations': []}
  with open(logfile, 'r') as infile:
    last_iteration_complete_time = None
    t0 = None
    for line in infile:
      if t0 is None and iteration_start_regex.search(line) is not None:
        t0 = _time(line)
        stats['algo_start_time'] = t0
        last_iteration_complete_time = t0
      ll_search = loglike_regex.search(line)
      if ll_search is not None:
        loglike = float(ll_search.group(1))
        t = _time(line)
        if last_iteration_complete_time is not None:
          delta_t = t - last_iteration_complete_time
        last_iteration_complete_time = t
        stats['iterations'].append({'loglike': loglike, 't': t, 'delta_t': delta_t})
  return stats
  

def extract_statistics(exp_dir):
  stats = defaultdict(list)
  for alg in os.listdir(exp_dir):
    print('....')
    print(alg)
    if not os.path.isdir(os.path.join(exp_dir, alg)):
      print('not dir', alg)
      continue
    for _try in os.listdir(os.path.join(exp_dir, alg)):
      try_logfile = os.path.join(exp_dir, alg, _try, 'log.txt')
      try_stats = extract_logfile_stats(try_logfile)
      stats[alg].append(try_stats)
      _mean = np.mean([x['delta_t'].total_seconds() for x in try_stats['iterations']])
      # print('{:.3f} seconds'.format(_mean))
  return stats


colors = 'r c m y k b g'.split()


def run_plot_loglike_over_iterations(args):
  stats = extract_statistics(args.experiment_dir)
  for k, (alg, alg_stats) in enumerate(stats.items()):
    # print(alg_stats)
    for i, alg_try_stats in enumerate(alg_stats):
      # print(alg_try_stats)
      ll = [x['loglike'] for x in alg_try_stats['iterations']]
      if len(ll):
        plt.plot(ll, label='{} try {}'.format(alg, i), color=colors[k])
  plt.xlabel('iterations')
  plt.ylabel('log likelihood of heldout data')
  plt.legend(loc='lower right')
  plt.savefig('loglike_vs_iteration.png', dpi=320)


def run_plot_loglike_over_time(args):
  stats = extract_statistics(args.experiment_dir)
  for k, (alg, alg_stats) in enumerate(stats.items()):
    # print(alg_stats)
    for i, alg_try_stats in enumerate(alg_stats):
      # print(alg_try_stats)
      ll = [x['loglike'] for x in alg_try_stats['iterations']]
      tt = [x['t'] for x in alg_try_stats['iterations']]
      tt_start_sec = [(t - tt[0]).total_seconds() for t in tt]
      if len(ll):
        plt.plot(tt_start_sec, ll, label='{} try {}'.format(alg, i), color=colors[k])
  plt.xlabel('time (seconds)')
  plt.ylabel('log likelihood of heldout data')
  plt.legend(loc='lower right')
  plt.savefig('loglike_vs_time.png', dpi=320)


def run_plot_iteration_distribution(args):
  stats = extract_statistics(args.experiment_dir)
  deltas = []
  for k, (alg, alg_stats) in enumerate(stats.items()):
    # print(alg_stats)
    for i, alg_try_stats in enumerate(alg_stats):
      # print(alg_try_stats)
      deltas.extend([(x['delta_t'].total_seconds(), alg, i) for x in alg_try_stats['iterations']])
  secs, algs, tries = zip(*deltas)
  df = pd.DataFrame()
  df['time_seconds'] = secs
  df['algorithm'] = algs
  df['try'] = tries
  
  sns.violinplot(y='algorithm', x='time_seconds', data=df)
  plt.savefig('iteration_times.png', dpi=320)

  means = df.groupby(['algorithm', 'try']).mean()
  means.rename(columns={'time_seconds': 'mean_seconds'}, inplace=True)
  medians = df.groupby(['algorithm', 'try']).median()
  medians.rename(columns={'time_seconds': 'median_seconds'}, inplace=True)
  stat_df = pd.concat([means, medians], axis=1)
  print(stat_df)
  stat_df.to_csv('stats.csv')


def _run(workload, _dir):
  config_filename = os.path.join(_dir, 'config.yaml')
  log_filename = os.path.join(_dir, 'log.txt')
  with open(config_filename, 'w') as outfile:
    yaml.dump(workload, outfile)
    os.system('cat {}'.format(config_filename))
    run_macrobase('pipeline', config_filename, save_log=log_filename)
    os.system('grep tuples {}'.format(log_filename))
    _command = 'grep point {}'.format(log_filename)
    os.system(_command)


def plot_violines(data):
  sns.violinplot(y='algorithm', x='ms', hue='tt', data=data)


def run_dump_stats(args):
  extract_statistics(args.experiment_dir)


# def run_plot_extracted(args):
#   extract_statistics(args.experiment_dir)
#   with open(os.path.join(args.experiment_dir, 'times.json')) as infile:
#     _json = json.load(infile)
#   what, algos, values = [], [], []
#   for time_type, _dict in _json.items():
#     for alg, _list in _dict.items():
#       what.extend(len(_list) * [time_type])
#       algos.extend(len(_list) * [alg])
#       values.extend(_list)
#   df = pd.DataFrame()
#   df['tt'] = what
#   df['algorithm'] = algos
#   df['ms'] = values
#   plot_violines(df)
#   if args.savefig:
#     plt.savefig(args.savefig, dpi=320)
#   else:
#     plt.show()


def run_extract_statistics(args):
  extract_statistics(args.experiment_dir)


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
  extract_statistics(exp_dir)


def run_single_experiment(args):
  experiment = yaml.load(THIS_EXPERIMENT_YAML.format(
    data=_filename_without_ext(args.data.name),
    algo=_filename_without_ext(args.algo.name)))
  print(experiment)
  experiment.update(yaml.load(args.data))
  experiment.update(yaml.load(args.algo))
  _dir = exp_bench_dir(experiment)
  _run(experiment, _dir)

  log_filename = os.path.join(_dir, 'log.txt')
  os.system('grep log {}'.format(log_filename))
  extract_logfile_stats(log_filename)

if __name__ == '__main__':
  args = parse_args()
  args.func(args)
