import argparse
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import patches
import re
import matplotlib.animation as animation
from algebra import get_ellipse_from_covariance


def parse_args(*argument_list):
  parser = argparse.ArgumentParser()
  parser.add_argument('log_file')
  args = parser.parse_args(*argument_list)
  return args


def update_atoms(lines, i):
  clusters = {}
  for i in range(i, len(lines)):
    if not re.search('macrobase.analysis.stats.mixture.NormalWishartClusters: \d+', lines[i]):
      return clusters
    print lines[i]
    ci = int(re.search('macrobase.analysis.stats.mixture.NormalWishartClusters: (\d)', lines[i]).group(1))
    weight = re.search('weight: (\d+.\d+)', lines[i]).group(1)
    mean = re.search('mean: {([-]?\d+.\d+; [-]?\d+.\d+)}', lines[i]).group(1)
    mean = [float(x) for x in mean.split(';')]
    covObj = re.search('cov: Array2DRowRealMatrix{{([-]?\d+.\d+,[-]?\d+.\d+)},{([-]?\d+.\d+,[-]?\d+.\d+)}}', lines[i])
    beta = float(re.search('beta: ([-]?\d+.\d+)', lines[i]).group(1))
    dof = float(re.search('dof: (\d+.\d+)', lines[i]).group(1))
    scale = (dof + 1 - 2) * beta / (1 + beta)
    print 'beta, dof, scale', beta, dof, scale
    cov = [[scale * float(x) for x in covObj.group(1).split(',')],
           [scale * float(x) for x in covObj.group(2).split(',')]]
    clusters[ci] = (mean, cov)
  return clusters


def update_weights(lines, i):
  coeffs_text = lines[i].split('coeffs:')[-1].strip()
  coeffs_text = coeffs_text.strip('[]')
  return [float(x) for x in coeffs_text.split(',')]


if __name__ == '__main__':
  args = parse_args()
  with open(args.log_file) as infile:
    lines = list(infile)
  updated_weights, updated_atoms = False, False
  data = []
  for i in range(len(lines)):
    if re.search('MultiComponents.(moveNatural|update)', lines[i]):
      weights = update_weights(lines, i+1)
      updated_weights = True
    if re.search('NormalWishartClusters.(moveNatural|update)', lines[i]):
      atoms = update_atoms(lines, i+1)
      updated_atoms = True
    if updated_weights and updated_atoms:
      data.append((weights, atoms))
      updated_weights, updated_atoms = False, False

  fig = plt.figure()
  ax = fig.add_subplot(111, aspect='equal')

  def original_clusters():
    clusters = [[(1.5,2),((0.5,0.4),(0.4,0.5)),50000],
                [(2,0),((0.3,0),(0,0.6)),30000],
                [(4.5,1),((0.9,0.2),(0.2,0.3)),20000]]
    artists = []
    for center, cov, weight in clusters:
      w, h, angle = get_ellipse_from_covariance(cov)
      e = patches.Ellipse(center, w, h, angle=angle, color='r')
      e.set_alpha(1. * weight / 60000)
      artists.append(e)
    return artists

  list_of_lists = []

  for weights, atoms in data:
    _list = []
    for i, (center, sigma) in atoms.iteritems():
      w, h, angle = get_ellipse_from_covariance(sigma)
      print w, h, (center, sigma), weights[i]
      e = patches.Ellipse(center, w, h, angle=angle, label='%d' % i)
      e.set_alpha(weights[i] / 70000)
      #ax.add_artist(e)
      _list.append(e)
    list_of_lists.append(_list)

  def init():
    print 'init()'
    ax.set_xlim([0, 6])
    ax.set_ylim([-1.5, 4.5])
    #ax.add_artist(list_of_lists[i][0])

  def animate(i):
    print 'animate', i
    ax.cla()
    arts = original_clusters()
    print arts
    for a in arts:
      ax.add_artist(a)
    for art in list_of_lists[i]:
      ax.add_artist(art)
    return list_of_lists[i]

  print 'ani'
  print 'len(..', len(list_of_lists)

  ani = animation.FuncAnimation(fig, animate, np.arange(1, len(list_of_lists)),
                                interval=1000, blit=False, init_func=init)

  # init()
  # print list_of_lists[0][0]
  # ax.add_artist(list_of_lists[0][0])
 #  ani = animation.ArtistAnimation(fig, list_of_lists, interval=50, blit=False,
 #                                  repeat_delay=1000)

  # init()
  # for a in add_actual_clusters():
  #   ax.add_artist(a)

  plt.show()
