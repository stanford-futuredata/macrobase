import argparse
import errno
import os
import yaml
from generate_multimodal_distribution import generate_distribution
from generate_multimodal_distribution import parse_args as generator_parse_args
from py_analysis.common import add_macrobase_args_dest_camel
from py_analysis.common import get_macrobase_camel_args
from py_analysis.plot_distribution import parse_args as plot_dist_parse_args
from py_analysis.plot_distribution import plot_distribution
from py_analysis.plot_estimator import parse_args as plot_est_parse_args
from py_analysis.plot_estimator import plot_estimator


def run_macrobase(cmd='batch', conf='conf/batch.conf', **kwargs):
  extra_args = ' '.join(['-Ddw.{key}={value}'.format(key=key, value=value)
                         for key, value in kwargs.items()])
  macrobase_cmd = '''java {extra_args} -Xms128m -Xmx16G \\
      -cp "src/main/resources/:target/classes:target/lib/*:target/dependency/*" \\
      macrobase.MacroBase {cmd} {conf_file}'''.format(
      cmd=cmd, conf_file=conf, extra_args=extra_args)
  print 'running the following command:'
  print macrobase_cmd
  os.system(macrobase_cmd)


def parse_args():
  parser = argparse.ArgumentParser()
  parser.add_argument('experiment_yaml', type=argparse.FileType('r'))
  parser.add_argument('--remove-cached-data', action='store_true')
  parser.add_argument('--rename', help='Give a new name to the experiment')
  parser.add_argument('--compile', action='store_true',
                      help='Run mvn compile before running java code.')
  add_macrobase_args_dest_camel(parser)
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
    experiment['macrobase']['taskName'] = args.rename
  else:
    experiment_name = os.path.basename(args.experiment_yaml.name).rsplit('.')[0]
    if args.outlierDetectorType:
      experiment_name = '%s-%s' % (experiment_name, args.outlierDetectorType)
    experiment['macrobase']['taskName'] = experiment_name

  taskname = experiment['macrobase']['taskName']
  if 'synthetic_data' in experiment:
    data_file = _file('target', 'data', taskname + '.csv')
    generate_args = generator_parse_args(
        ['2D'] +
        experiment['synthetic_data'].split() +
        ['--outfile', data_file])
    generate_distribution(generate_args)

    figure_file = _file('target', 'plots', '%s-distribution.png' % taskname)
    plot_args = plot_dist_parse_args(['--csv', data_file,
                                      '--hist2d', 'XX', 'YY',
                                      '--savefig', figure_file,
                                      '--x-limits', '1', '25',
                                      '--y-limits', '1', '25'])
    plot_distribution(plot_args)
  else:
    print 'it has to be a synthetic experiment for now'

  with open('conf/batch.yaml', 'r') as infile:
    config = yaml.load(infile)
    config.update(experiment['macrobase'])
    config['storeScoreDistribution'] = taskname + '.json'
    if 'synthetic_data' in experiment:
      config['dbUrl'] = data_file

  config_file = _file('conf', 'custom', taskname + '.yaml')
  with open(config_file, 'w') as config_yaml:
    config_yaml.write(yaml.dump(config))

  kwargs = get_macrobase_camel_args(args)  
  run_macrobase(conf=config_file, **kwargs)

  raw_args = [
       '--estimates', os.path.join('target', 'scores', config['storeScoreDistribution']),
       '--savefig']
  if len(config['targetHighMetrics']) == 2:
    raw_args.extend([
         '--restrict-to', 'outliers',
         '--x-limits', '1', '25',
         '--y-limits', '1', '25'])
  estimator_args = plot_est_parse_args(raw_args)
  print 'running following plot command:'
  print 'python script/py_analysis/plot_estimator.py ' + ' '.join(raw_args)
  plot_estimator(estimator_args)
