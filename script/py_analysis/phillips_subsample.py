import argparse
from random import sample
import pandas as pd


def parse_args(*argument_list):
  parser = argparse.ArgumentParser()
  parser.add_argument('csv', type=argparse.FileType('r'))
  parser.add_argument('outcsv', type=argparse.FileType('w'))
  parser.add_argument('--points-per-controller', type=int, default=2000)
  args = parser.parse_args(*argument_list)
  return args


if __name__ == '__main__':
  args = parse_args()
  df = pd.read_csv(args.csv)
  distinct_controllers = set(df['controller_id'])
  print 'total controller_ids included =', len(distinct_controllers)

  sampled_df = None
  frames = []
  for cid in distinct_controllers:
    cntl_df = df[df['controller_id'] == cid]
    x = cntl_df.loc[sample(cntl_df.index, min(cntl_df.shape[0], args.points_per_controller))]
    frames.append(x)

  sampled_df = pd.concat(frames)
  print 'dumpnig total %d rows' % sampled_df.shape[0]
  sampled_df.to_csv(args.outcsv, index=False)
