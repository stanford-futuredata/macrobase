import os
from time import strftime
import errno


def makedirs_p(lowest_directory):
  try:
    os.makedirs(lowest_directory)
  except Exception as e:
    if e.errno != errno.EEXIST:
      raise e


def exp_bench_dir(workload):
  sub_dir = os.path.join(
    os.getcwd(),
    'bench',
    'workflows',
    workload['macrobase.query.name'],
    strftime('%m-%d-%H:%M:%S'))
  makedirs_p(sub_dir)
  return sub_dir
