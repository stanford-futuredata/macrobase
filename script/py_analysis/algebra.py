import numpy as np
import matplotlib.pyplot as plt
from matplotlib import patches


def get_ellipse_from_covariance(matrix):
  values, vectors = np.linalg.eig(matrix)
  maxI = np.argmax(values)
  large, small = values[maxI], values[1 - maxI]
  return large, small, 0.5 * np.rad2deg(np.arccos(vectors[0, 0]))


if __name__ == '__main__':
  matrix = [
    [
      14.610641797918163,
      -3.878610383631345
    ],
    [
      -3.8786103836313455,
      2.032210715533014
    ]
  ]
  matrix2 = [      [
        0.679288224450315,
        0.21604946827925653
      ],
      [
        0.2160494682792565,
        10.091504208107827
      ]]
  matrix3 = [[2, 1.9], [1.9, 2]]
  w, h, angle = get_ellipse_from_covariance(matrix)
  print w, h, angle
  e = patches.Ellipse((0, 0), w, h, angle=angle)
  fig = plt.figure(0)
  ax = fig.add_subplot(111, aspect='equal')
  ax.add_artist(e)
  w, h, angle = get_ellipse_from_covariance(matrix2)
  print w, h, angle
  e2 = patches.Ellipse((10, 10), w, h, angle=angle)
  ax.add_artist(e2)
  w, h, angle = get_ellipse_from_covariance(matrix3)
  print w, h, angle
  e3 = patches.Ellipse((-10, -10), w, h, angle=angle)
  ax.add_artist(e3)
  ax.set_xlim(-20, 20)
  ax.set_ylim(-20, 20)
  plt.show()
