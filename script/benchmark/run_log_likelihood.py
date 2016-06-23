import argparse
import yaml
from macrobase_cmd import run_macrobase
from helpers import make_file
from common import add_macrobase_args
from constants import QUERY_NAME


def parse_args():
  parser = argparse.ArgumentParser()
  parser.add_argument('experiment_yaml', type=argparse.FileType('r'))
  add_macrobase_args(parser)
  return parser.parse_args()


if __name__ == '__main__':
  args = parse_args()
  experiment = yaml.load(args.experiment_yaml)

  taskname = experiment['macrobase'][QUERY_NAME]

  with open('conf/batch.yaml', 'r') as infile:
    config = yaml.load(infile)
    config.update(experiment['macrobase'])

  config_file = make_file('conf', 'custom', taskname + '.yaml')
  with open(config_file, 'w') as config_yaml:
    config_yaml.write(yaml.dump(config))

  kwargs = getattr(args, 'macrobase_args', {})
  run_macrobase(conf=config_file, **kwargs)
