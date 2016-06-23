import argparse
import matplotlib.pyplot as plt
import matplotlib.mlab as mlab
from plot_score_contours import load_json_dump
from algebra import get_ellipse_from_covariance
from matplotlib.colors import LogNorm
import pandas as pd
import json
import numpy as np
from plot_score_contours import load_cluster_parameters
from cluster_demo_2d import extract_all_groups
from cluster_demo_2d import pretty_print_group
from matplotlib import patches
import re


def parse_args(*argument_list):
  parser = argparse.ArgumentParser()
  parser.add_argument('log_file')
  parser.add_argument('--exp', default='phillips-power_change')
  parser.add_argument('--weight-cap', type=float)
  args = parser.parse_args(*argument_list)
  if args.exp:
    args.centers = 'target/scores/centers-{}-mixtures.json'.format(args.exp)
    args.weights = 'target/scores/weights-{}-mixtures.json'.format(args.exp)
    args.covariances = 'target/scores/covariances-{}-mixtures.json'.format(args.exp)
  return args


def ax_plot_hist2d(ax, dataframe, xlabel, ylabel):
  df = dataframe
  df = df[df[xlabel].notnull()][df[ylabel].notnull()]
  ax.hist2d(df[xlabel], df[ylabel], 100, norm=LogNorm())
  ax.set_xlim([0, 65])
  ax.set_xlabel(xlabel)
  ax.set_ylabel(ylabel)


def shape_data(scored_grid, score_cap=None, score_lower_limit=None):
  X, Y, Z = load_json_dump(scored_grid)

  if score_cap:
    Z = [min(z, score_cap) for z in Z]
  if score_lower_limit:
    Z = [max(z, score_lower_limit) for z in Z]

  size = int(np.sqrt(len(Z)))
  X = np.reshape(X, (size, size))
  Y = np.reshape(Y, (size, size))
  Z = np.reshape(Z, (size, size))
  return X, Y, Z


def get_closest_cluster(centers, x, y):
  index = -1
  min_dist = 1e10
  for i, (mu,) in enumerate(centers):
    d2 = (mu - x) ** 2
    if d2 < min_dist:
      index, min_dist = i, d2
  return index


def sanitize(x):
  try:
    if type(x[2]) is str:
      if 'city' in x[3]:
        return '\tcid = {:10s}\n\tcity = {}\n\tratio = {}\n\tin cluster points = {:d}'.format(x[3]['controller_id'], x[3]['city'], x[2], x[1])
      else:
        return '\tcid = {:10s}}\n\tratio = {}\n\tin cluster points = {:d}'.format(x[3]['controller_id'], x[3]['city'], x[2], x[1])
    # return '%s, %s\n%.2f %d' % (x[3]['controller_id'], x[3]['city'], x[2], x[1])
    return '\tcid = {:10s}\n\tcity = {}\n\tratio = {:.2f}\n\tin cluster points = {:d}'.format(x[3]['controller_id'], x[3]['city'], x[2], x[1])
  except:
    return repr(x)


if __name__ == '__main__':
  args = parse_args()
  all_groups = extract_all_groups(args.log_file)
  # with open('formateed_output.txt', 'w') as outfile:
  #   json.dump(all_groups, outfile)
  centers = [tuple(x['data']) for x in json.load(open(args.centers))]
  sigmas = load_cluster_parameters(args.covariances)
  with open(args.weights) as infile:
    weights = json.load(infile)
  for g, mu, sigma, w in zip(all_groups, centers, sigmas, weights):
    if args.weight_cap is not None and w < args.weight_cap:
      continue
    print '-'
    print mu, sigma, w
    if g:
      pretty_print_group(g)
    print '-'

  fig, (ax1, ax2) = plt.subplots(1, 2, sharey=True)
  fig.subplots_adjust(wspace=0)
  plt.setp([a.get_yticklabels() for a in fig.axes[1:]], visible=False)

  def onclick(event):
    index = get_closest_cluster(centers, event.xdata, event.ydata)
    if event.inaxes != ax1 or index < 0:
      print index
      print '!!!!!!!'
      return
    ax2.cla()
    ax2.set_title('cluster %d, center = %.5f' % (index, centers[index][0]))
    ax1.cla()
    xx = np.linspace(-6, 6, 1000)
    for i, (w, mu, sigma) in enumerate(zip(weights, centers, sigmas)):
      # xx = np.linspace(mu[0] - 10 * sigma[0][0], mu[0] + 10 * sigma[0][0], 100)
      z = w * mlab.normpdf(xx, mu[0], sigma[0][0])
      line = ax1.plot(xx, z, color='g')
      if i == index:
        plt.setp(line, linewidth=2, color='r')
    ax1.set_xlim([-6, 6])
    ax1.set_ylim([0, 1])
    ax2.xaxis.set_visible(False)
    ax2.yaxis.set_visible(False)
    print index
    total = min(6, len(all_groups[index]))
    for j, x in enumerate(all_groups[index]):
      ax2.text(0, 8.8 - 10. * j / total, sanitize(x), fontsize=24)
    fig.canvas.draw()

  cid = fig.canvas.mpl_connect('button_press_event', onclick)

  plt.show()
