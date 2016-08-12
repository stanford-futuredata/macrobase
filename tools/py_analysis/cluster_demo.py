import argparse
import matplotlib.pyplot as plt
from plot_score_contours import load_json_dump
from algebra import get_ellipse_from_covariance
from matplotlib.colors import LogNorm
import pandas as pd
import json
import numpy as np
from plot_score_contours import load_cluster_parameters
from matplotlib import patches
import re


def parse_args(*argument_list):
  parser = argparse.ArgumentParser()
  parser.add_argument('log_file')
  parser.add_argument('--csv', required=True)
  parser.add_argument('--scored-grid', required=True)
  parser.add_argument('--hist2d', nargs=2, required=True)
  parser.add_argument('--exp', default='phillips-Nrtu-mixtures')
  args = parser.parse_args(*argument_list)
  if args.exp:
    args.centers = 'target/scores/centers-{}.json'.format(args.exp)
    args.weights = 'target/scores/weights-{}.json'.format(args.exp)
    args.covariances = 'target/scores/covariances-{}.json'.format(args.exp)
  return args

new_set_regex = re.compile('outliers:')
support_regex = re.compile('support: (\d+[.]\d+)')
records_regex = re.compile('records: (\d+[.]\d+)')
ratio_regex = re.compile('ratio: (\d+[.]\d+)')
infinity_ratio_regex = re.compile('ratio: Infinity')
column_regex = re.compile(r'(combined_rtu_mode|lighting_state|datasource_id|controller_id|city)')
separator_regex = re.compile('-----')


def extract_group(lines, start):
  outliers = []
  for x in range(start, len(lines)):
    if not separator_regex.search(lines[x]):
      continue
    support, records, ratio, attributes = None, None, None, {}
    for i in range(x + 1, len(lines)):
      if separator_regex.search(lines[i]):
        if support is None:
          return outliers
        outliers.append((support, records, ratio, attributes))
        support, records, ratio, attributes = None, None, None, {}
      elif support_regex.search(lines[i]):
        support = float(support_regex.search(lines[i]).group(1))
      elif records_regex.search(lines[i]):
        records = int(float(records_regex.search(lines[i]).group(1)))
      elif column_regex.search(lines[i]):
        key, value = lines[i].strip().split(': ')
        attributes[key] = value
      elif ratio_regex.search(lines[i]):
        ratio = float(ratio_regex.search(lines[i]).group(1))
      elif infinity_ratio_regex.search(lines[i]):
        ratio = 'Infinity'
      elif new_set_regex.search(lines[i]):
        return outliers


def extract_all_groups(log_file):
  all_groups = []
  with open(log_file, 'r') as infile:
    lines = list(infile)
  for i in xrange(0, len(lines)):
    if new_set_regex.search(lines[i]):
      all_groups.append(extract_group(lines, i))
  return all_groups


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


def contours(ax, X, Y, Z):
  CS = ax.contour(X, Y, Z, linewidth=10000, inline=0)


def get_closest_cluster(centers, x, y):
  index = -1
  min_dist = 1e10
  for i, (c_x, c_y) in enumerate(centers):
    d2 = (c_x - x) ** 2 + (c_y - y) ** 2
    if d2 < min_dist:
      index, min_dist = i, d2
  return index


def sanitize(x):
  print(x)
  if 'controller_id' not in x[3] or 'city' not in x[3]:
    return '\n'.join(['\t{} = {}'.format(key, value) for key, value in x[3].items()] + ['\t{}'.format(y) for y in [x[2], x[1]]])
  if type(x[2]) is str:
    return '\tcid = {:10s}\n\tcity = {}\n\tratio = {}\n\tin cluster points = {:d}'.format(x[3]['controller_id'], x[3]['city'], x[2], x[1])
  # return '%s, %s\n%.2f %d' % (x[3]['controller_id'], x[3]['city'], x[2], x[1])
  return '\tcid = {:10s}\n\tcity = {}\n\tratio = {:.2f}\n\tin cluster points = {:d}'.format(x[3]['controller_id'], x[3]['city'], x[2], x[1])

if __name__ == '__main__':
  args = parse_args()
  all_groups = extract_all_groups(args.log_file)
  # with open('formateed_output.txt', 'w') as outfile:
  #   json.dump(all_groups, outfile)
  centers = [tuple(x['data']) for x in json.load(open(args.centers))]
  sigmas = load_cluster_parameters(args.covariances)
  with open(args.weights) as infile:
    weights = json.load(infile)
  artists = []
  orig_artists = []
  for i in range(len(centers)):
    w, h, angle = get_ellipse_from_covariance(sigmas[i])
    e = patches.Ellipse(centers[i], w, h, angle=angle)
    e_copy = patches.Ellipse(centers[i], w, h, angle=angle)
    e.set_alpha(np.power(weights[i], .4))
    e_copy.set_alpha(weights[i])
    artists.append(e)
    orig_artists.append(e_copy)

  X, Y, Z = shape_data(args.scored_grid)

  # fig = plt.figure()
  # ax1 = fig.add_subplot(131)
  # ax2 = fig.add_subplot(133)
  fig, (ax1, ax2) = plt.subplots(1, 2, sharey=True)
  fig.subplots_adjust(wspace=0)
  plt.setp([a.get_yticklabels() for a in fig.axes[1:]], visible=False)

  data = pd.read_csv(args.csv)
  small_data = data.loc[np.random.choice(data.index, 500000, replace=False)]
  ax_plot_hist2d(ax1, small_data, *args.hist2d)
  for e in orig_artists:
    ax1.add_artist(e)
  contours(ax1, X, Y, Z)

  def onclick(event):
    i = get_closest_cluster(centers, event.xdata, event.ydata)
    if event.inaxes != ax1 or i < 0:
      return
    ax2.cla()
    ax2.set_title('cluster %d, center = (%.1f, %.1f)' % (i, centers[i][0], centers[i][1]))
    e = artists[i]
    ax1.cla()
    ax_plot_hist2d(ax1, small_data, *args.hist2d)
    ax1.add_artist(e)
    for e in orig_artists:
      ax1.add_artist(e)
    contours(ax1, X, Y, Z)
    ax2.xaxis.set_visible(False)
    ax2.yaxis.set_visible(False)
    for j, x in enumerate(all_groups[i]):
      ax2.text(0, 20.3 - 24. * j / len(all_groups[i]), sanitize(x), fontsize=24)
    fig.canvas.draw()

  cid = fig.canvas.mpl_connect('button_press_event', onclick)

  plt.show()
