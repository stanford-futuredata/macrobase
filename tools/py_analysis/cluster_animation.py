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
  parser.add_argument('--save')
  parser.add_argument('--update-interval', type=int, default=500,
                      help='interval between frames in milliseconds')
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


def update_atoms_easy(lines, i):
  center_regex = re.compile('macrobase.analysis.stats.mixture.VariationalInference: centers = (.*)')
  cov_regex = re.compile('macrobase.analysis.stats.mixture.VariationalInference: covariances = (.*)')
  matrix_regex = re.compile('Array2DRowRealMatrix{{([-]?\d+.\d+,[-]?\d+.\d+)},{([-]?\d+.\d+,[-]?\d+.\d+)}}')
  vector_regex = re.compile('{([-]?\d+(.\d+)?; [-]?\d+(.\d+)?)}')

  def to_matrix(string_tuple):
    return [[float(x) for x in string.split(',')] for string in string_tuple]

  def to_vector(vector_string):
    return [float(x) for x in vector_string.split(';')]

  for i in range(i, len(lines)):
    if center_regex.search(lines[i]):
      centers = [to_vector(x[0]) for x in vector_regex.findall(lines[i])]
      print 'centers:', centers
    elif cov_regex.search(lines[i]):
      covs = [to_matrix(x) for x in matrix_regex.findall(lines[i])]
    else:
      return zip(*(centers, covs))


def update_weights(lines, i):
  coeffs_text = lines[i].split('weights = ')[-1].strip()
  coeffs_text = coeffs_text.strip('[]')
  return [float(x) for x in coeffs_text.split(',')]


def convert_into_ellipses(data):
  list_of_lists = []
  for weights, atoms in data:
    _list = []
    for i, (center, sigma) in enumerate(atoms):
      w, h, angle = get_ellipse_from_covariance(sigma)
      e = patches.Ellipse(center, w, h, angle=angle, label='%d' % i)
      e.set_alpha(weights[i])
      _list.append(e)
    list_of_lists.append(_list)
  return list_of_lists


def extract_data(lines, atom_regex, atom_updater, mixing_regex, mixing_updater):
  updated_weights, updated_atoms = False, False
  data = []
  for i in range(len(lines)):
    if atom_regex.search(lines[i]):
      atoms = atom_updater(lines, i)
      updated_atoms = True
    elif mixing_regex.search(lines[i]):
      weights = mixing_updater(lines, i)
      updated_weights = True
    if updated_weights and updated_atoms:
      data.append((weights, atoms))
      print 'weights: ', weights
      for a in atoms[:3]:
        print a
      updated_weights, updated_atoms = False, False
  return data


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


def plot_per_iteration(args):
  atoms_regex = re.compile('centers = ')
  mixing_regex = re.compile('weights = ')
  list_of_lists = []
  fig = plt.figure()
  plt.xlabel('power_usage')
  plt.ylabel('time of day')
  ax = fig.add_subplot(111)

  with open(args.log_file) as infile:
    lines = list(infile)
  data = extract_data(lines, atoms_regex, update_atoms_easy, mixing_regex, update_weights)
  list_of_lists = convert_into_ellipses(data)

  def init():
    ax.set_xlim([0, 70])
    ax.set_ylim([0, 24])

  def animate(i):
    print 'animate', i
    ax.cla()
    ax.set_xlabel('power_usage')
    ax.set_ylabel('time of day')
    ax.set_title('%d' % i)
    # arts = original_clusters()
    # for a in arts:
    #   ax.add_artist(a)
    for art in list_of_lists[i]:
      ax.add_artist(art)
    return list_of_lists[i]

  print 'len', len(list_of_lists)
  ani = animation.FuncAnimation(fig, animate, np.arange(0, len(list_of_lists)),
                                interval=args.update_interval, blit=False,
                                init_func=init)
  if args.save:
    ani.save(args.save)
  else:
    plt.show()


if __name__ == '__main__':
  args = parse_args()
  plot_per_iteration(args)
