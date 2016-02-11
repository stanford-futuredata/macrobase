import argparse
import errno
import os
import yaml
from generate_multimodal_distribution import generate_distribution
from generate_multimodal_distribution import parse_args as generator_parse_args
from py_analysis.plot_distribution import parse_args as plot_dist_parse_args
from py_analysis.plot_distribution import plot_distribution
from py_analysis.plot_estimator import parse_args as plot_est_parse_args
from py_analysis.plot_estimator import plot_estimator


def run_macrobase(cmd='batch', conf='conf/batch.conf'):
  macrobase_cmd = '''java -Xms128m -Xmx16G \\
      -cp "src/main/resources/:target/classes:target/lib/*:target/dependency/*" \\
      macrobase.MacroBase {cmd} {conf_file}'''.format(
      cmd=cmd, conf_file=conf)
  print 'running the following command:'
  print macrobase_cmd
  os.system(macrobase_cmd)


def parse_args():
  parser = argparse.ArgumentParser()
  parser.add_argument('experiment_yaml', type=argparse.FileType('r'))
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
  experiment = yaml.load(args.experiment_yaml)
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
    os._exit(1)

  with open('conf/batch.yaml', 'r') as infile:
    config = yaml.load(infile)
    config.update(experiment['macrobase'])
    config['storeScoreDistribution'] = taskname + '.json'
    if 'synthetic_data' in experiment:
      config['dbUrl'] = data_file

  config_file = _file('conf', 'custom', taskname + '.yaml')
  with open(config_file, 'w') as config_yaml:
    config_yaml.write(yaml.dump(config))

  run_macrobase(conf=config_file)

  estimator_args = plot_est_parse_args(
      ['--estimates', os.path.join('target', 'scores', config['storeScoreDistribution']),
       '--hist2d', 'outliers',
       '--savefig', _file('target', 'plots', '%s-outliers.png' % taskname),
       '--x-limits', '1', '25',
       '--y-limits', '1', '25'])
  plot_estimator(estimator_args)
