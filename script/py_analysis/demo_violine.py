import pandas as pd
import numpy as np
import argparse
import seaborn as sns
import matplotlib.pyplot as plt


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--csv', required=True)
  parser.add_argument('--context', default='poster',
                      help='Modifies size of legend, axis, etc.')
  args = parser.parse_args()
  df = pd.read_csv(args.csv)
  dff = df.replace([-np.inf, np.inf], np.nan).dropna(subset=['log10_interval_seconds'])
  sns.set_context(args.context)
  plt.figure(figsize=(20, 15))
  sns.violinplot(y='log10_interval_seconds', x='transition', data=dff)
  plt.savefig('violin_log10_interval_seconds.png', dpi=320)
  print('saved figure to violin_log10_interval_seconds.png')
