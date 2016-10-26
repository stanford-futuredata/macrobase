import functools
import os
import errno
from time import strftime


def retry_if_fails(max_retries):
  """
  A decorator that will rerun the function if it fails (throws an exception)
  until the function succeeds or max_retries retries are attempted.
  e.g. retry_if_fails(0) decorator will not alter the behaviour of the funciton
  """
  def decorator(function):
    @functools.wraps(function)
    def wrapper(*args, **kwargs):
      for _ in range(max_retries):
        try:
          return function(*args, **kwargs)
        except:
          print 'pass'
          pass
      return function(*args, **kwargs)
    return wrapper
  return decorator


retry_3_times = retry_if_fails(3)


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
    strftime('%m-%d-%H_%M_%S'))
  makedirs_p(sub_dir)
  return sub_dir
