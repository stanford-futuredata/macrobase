import sklearn.mixture
import argparse
import sys
import pandas as pd
import numpy as np


def parse_args(*argument_list):
  parser = argparse.ArgumentParser()
  parser.add_argument('sklearn_class', default='VBGMM',
                      choices=['DPGMM', 'VBGMM', 'GMM'])
  args = parser.parse_args(*argument_list)
  return args

args = parse_args()
model_class = getattr(sklearn.mixture, args.sklearn_class)
if args.sklearn_class == 'GMM':
  gmm = model_class(n_components=3, covariance_type='diag',
                    n_iter=20,
                    params='wmc', init_params='wmc')
else:
  gmm = model_class(n_components=3, covariance_type='diag', alpha=1.0,
                    n_iter=20,
                    params='wmc', init_params='wmc')

x = pd.read_csv('/Users/arsen/Workspace/data/3gaussians-1D-400p-far.csv')['XX']

with open('/Users/arsen/Workspace/data/3gaussians-1D-400p-far.txt') as infile:
  for line in infile:
    print(line.strip())
  print('')

x = np.reshape(x, (len(x), 1))

gmm.fit(x)

p = gmm.predict_proba(x)
print(np.sum(p, axis=0))

s = gmm.score(x)
print(np.mean(s))

if not gmm.converged_:
  print('did not converge')
  sys.exit(1)

print('weights: ', gmm.weights_)
print('means: ', gmm.means_.flatten())
# print('precisions:', gmm.precs_)
if args.sklearn_class == 'GMM':
  assert (1,) == gmm.covars_[0].shape
  stds_ = [np.sqrt(cov[0]) for cov in gmm.covars_]
  print('stds: ', stds_)
else:
  assert (1,) == gmm.precs_[0].shape
  stds_ = [np.sqrt(1. / prec[0]) for prec in gmm.precs_]
  print('stds: ', stds_)
