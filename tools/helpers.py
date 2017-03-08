import functools


def retry_if_fails(max_retries):
  """
  A decorator that will rerun the function if it fails (throws an exception)
  until the function succeeds or max_retries retries are attempted.
  e.g. retry_if_fails(0) decorator will not alter the behaviour of the function
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
