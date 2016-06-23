from plot_score_contours import parse_args
from plot_score_contours import load_cluster_parameters
import pandas as pd
import matplotlib.pyplot as pyplot
from common import set_plot_limits
import numpy as np
import json
import matplotlib.mlab as mlab


def load_dumped_scores(filename):
  with open(filename) as infile:
    data = json.load(infile)
  P, Z = [], []
  for point in data:
    P.append(point['metrics']['data'])
    if False and args.logscore:
      Z.append(np.exp(point['score']))
    else:
      Z.append(point['score'])
  return Z, zip(*P)


def plot_score_lines(args):

  Z, (X,) = load_dumped_scores(args.scored_grid)
  if args.histogram:
    data = pd.read_csv(args.csv)
    x = data[(data[args.histogram] > args.x_limits[0]) &
             (data[args.histogram] < args.x_limits[1])][args.histogram]
    n, bins, patches = pyplot.hist(x, bins=args.histogram_bins,
                                   label=args.histogram)
    N = x.shape[0]
    delta = bins[1] - bins[0]
    line = pyplot.plot(X, N * delta * np.exp(Z))
    pyplot.setp(line, linewidth=2, color='r')
    pyplot.xlabel(args.histogram)
  else:
    pyplot.plot(X, np.exp(Z))

  if args.centers and args.covariances and args.weights:
    weights = json.load(args.weights)
    weights = [w / sum(weights) for w in weights]
    centers = load_cluster_parameters(args.centers)
    sigmas = load_cluster_parameters(args.covariances)
    _min, _max = args.x_limits if args.x_limits else (min(X), max(X))
    print _min, _max
    xx = np.linspace(_min, _max, 1000)
    for w, mu, sigma in zip(weights, centers, sigmas):
      print mu, sigma
      pyplot.plot(xx, w * mlab.normpdf(xx, mu[0], sigma[0][0]), color='g')
    print weights, centers, sigmas

  set_plot_limits(pyplot, args)
  pyplot.show()

if __name__ == '__main__':
  args = parse_args()
  plot_score_lines(args)
