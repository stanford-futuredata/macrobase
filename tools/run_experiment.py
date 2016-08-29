"""
A script that runs yaml files as standalone experiments from
bench/experiment directory.

The default behavior is good enough to run the experiments, e.g.
 python tools/run_experiment.py tools/bench/experiments/3gaussians-ok.yaml

Experiment will run macrobase with a specified configuration, and
plot desire plots after the run is finished.

Command line arguments to this script let you customize the experiment
without changing the config file, which is very useful when diagnosing
the behavior with different configurations.
"""
import argparse
import errno
import os
import yaml
from generate_multimodal_distribution import generate_distribution
from generate_multimodal_distribution import parse_args as generator_parse_args
from py_analysis.common import add_macrobase_args
from py_analysis.plot_distribution import parse_args as plot_dist_parse_args
from py_analysis.plot_distribution import plot_distribution
from py_analysis.plot_estimator import parse_args as plot_est_parse_args
from py_analysis.plot_estimator import plot_estimator
from py_analysis.plot_score_contours import parse_args as plot_contour_parse_args  # noqa
from py_analysis.plot_score_contours import plot_score_contours
from macrobase_cmd import run_macrobase
from helpers import retry_3_times

SCORE_DUMP_FILE_CONFIG_PARAM = "macrobase.analysis.results.store"
HIGH_METRICS = "macrobase.loader.targetHighMetrics"
DETECTOR = "macrobase.analysis.detectorType"
DUMP_SCORE_GRID = 'macrobase.diagnostic.dumpScoreGrid'
DUMP_MIXTURES = 'macrobase.diagnostic.dumpMixtureComponents'
SCORED_DATA_FILE = "macrobase.diagnostic.scoreDataFile"
QUERY_NAME = "macrobase.query.name"
PIPELINE = 'macrobase.pipeline.class'

SAVEFIG_INFER_VALUE = 'INFER_SAVEFIG_FILENAME'


def parse_args():
  parser = argparse.ArgumentParser("""Example usage:
    python script/run_experiment.py bench/conf/treekde-test.yaml
  """)
  parser.add_argument('experiment_yaml', type=argparse.FileType('r'))
  parser.add_argument('--remove-cached-data', action='store_true')
  parser.add_argument('--rename', help='Give a new name to the experiment')
  parser.add_argument('--title-add', help='A title modifier')
  parser.add_argument('--compile', action='store_true',
                      help='Run mvn compile before running java code.')
  parser.add_argument('--profile', action='store_true',
                      help='Run with yourkit profiler attached.')
  parser.add_argument('--savefig', nargs='?', const=SAVEFIG_INFER_VALUE)
  add_macrobase_args(parser)
  return parser.parse_args()


def _file(*args):
  """ Wrapper around os.path.join and os.makedirs."""
  filename = os.path.join(*args)
  _makedirs_for_file(filename)
  return filename


def _makedirs_for_file(filename):
  try:
    os.makedirs(os.path.dirname(filename))
  except Exception as e:
    if e.errno != errno.EEXIST:
      raise e


def construct_title(config_dict, cmd_args, raw_plot_args):
  if QUERY_NAME in config_dict:
    return '{} ({})'.format(config_dict[QUERY_NAME], args.title_add)


if __name__ == '__main__':
  args = parse_args()
  if args.compile:
    status = os.system('mvn compile')
    if status != 0:
      os._exit(status)

  if args.remove_cached_data:
    os.system('rm cache/*')

  experiment = yaml.load(args.experiment_yaml)
  # Set a user specified experiment name, or pick a reasonable one.
  if args.rename is not None:
    experiment['macrobase'][QUERY_NAME] = args.rename
  elif QUERY_NAME not in experiment['macrobase']:
    experiment_name = os.path.basename(
        args.experiment_yaml.name).rsplit('.')[0]
    if 'macrobase_args' in vars(args) and DETECTOR in args.macrobase_args:
      experiment_name = '%s-%s' % (experiment_name,
                                   args.macrobase_args[DETECTOR])
    experiment['macrobase'][QUERY_NAME] = experiment_name

  taskname = experiment['macrobase'][QUERY_NAME]
  if 'synthetic_data' in experiment:
    data_file = _file('target', 'data', taskname + '.csv')

    @retry_3_times
    def generate_and_plot_data():
      if len(experiment['synthetic_data'].split()[1].split(',')) == 3:
        dimensions = '1D'
      else:
        dimensions = '2D'
      raw_args = (
          [dimensions] +
          experiment['synthetic_data'].split() +
          ['--outfile', data_file])
      print 'generating distribution with args:'
      print('python script/generate_multimodal_distribution.py ' +
            ' '.join(raw_args))
      generate_args = generator_parse_args(raw_args)
      generate_distribution(generate_args)

      figure_file = _file('target', 'plots', '%s-distribution.png' % taskname)
      raw_args = ['--csv', data_file,
                  '--savefig', figure_file]

      if dimensions == '2D':
        raw_args.extend(['--hist2d', 'XX', 'YY',
                         '--x-limits', '1', '25'])
      elif dimensions == '1D':
        raw_args.extend(['--histogram', 'XX'])
      print ('python script/py_analysis/plot_distribution.py ' +
             ' '.join(raw_args))

      plot_args = plot_dist_parse_args(raw_args)
      plot_distribution(plot_args)
    generate_and_plot_data()
  else:
    print 'it has to be a synthetic experiment for now'

  with open('conf/batch.yaml', 'r') as infile:
    config = yaml.load(infile)
    config.update(experiment['macrobase'])
    config[SCORE_DUMP_FILE_CONFIG_PARAM] = taskname + '.json'
    config[SCORED_DATA_FILE] = '{name}-scored.json'.format(name=taskname)
    config[DUMP_SCORE_GRID] = '{name}-grid.json'.format(name=taskname)
    config[DUMP_MIXTURES] = '{name}-mixtures.json'.format(name=taskname)
    if 'synthetic_data' in experiment:
      config['dbUrl'] = data_file
    if PIPELINE not in config:
      config[PIPELINE] = 'macrobase.analysis.pipeline.GridDumpingPipeline'

  config_file = _file('conf', 'custom', taskname + '.yaml')
  with open(config_file, 'w') as config_yaml:
    config_yaml.write(yaml.dump(config))

  kwargs = getattr(args, 'macrobase_args', {})
  print kwargs
  if args.profile:
    kwargs['profiler'] = 'yourkit'
  run_macrobase(conf=config_file, **kwargs)

  for script_dict in experiment.get('post_run', []):
    [(script, raw_args)] = script_dict.items()
    if script == 'plot_estimator':
      script_args = raw_args.split() + [
        '--estimates', os.path.join('target', 'scores',
                                    config[SCORE_DUMP_FILE_CONFIG_PARAM]),
        '--title', construct_title(config, args, raw_args)]
      print 'running following plot command:'
      print ('python script/py_analysis/plot_estimator.py ' +
             ' '.join(script_args))
      estimator_args = plot_est_parse_args(script_args)
      try:
        plot_estimator(estimator_args)
      except:
        pass
    elif script == 'plot_score_contours':
      script_args = raw_args.split() + [
        '--scored-grid', os.path.join('target', 'scores',
                                      config[DUMP_SCORE_GRID])]
      print 'running following plot command:'
      print ('python script/py_analysis/plot_score_contours.py ' +
             ' '.join(script_args))
      contour_args = plot_contour_parse_args(script_args)
      try:
        plot_score_contours(contour_args)
      except:
        pass
