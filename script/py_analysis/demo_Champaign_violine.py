import pandas as pd
import numpy as np
import argparse
import seaborn as sns
import matplotlib.pyplot as plt


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--csv')
  parser.add_argument('--context', default='poster',
                      help='Modifies size of legend, axis, etc.')
  parser.add_argument('--city', default='Champaign')
  args = parser.parse_args()

  df = pd.read_csv('target/phillips_data/power_usage_city_state_rtus.csv')
  df[args.city] = ((df['city'] == args.city)
                   .replace(True, args.city)
                   .replace(False, 'entire dataset'))
  df['daytime'] = (((df['time'] < 18) & (df['time'] > 4))
                   .replace(True, 'day')
                   .replace(False, 'night'))
  sns.set_context(args.context)
  plt.figure(figsize=(20, 15))
  sns.violinplot(y='daytime', x='power_usage', hue=args.city, orient='h', data=df[df['power_usage'] < 50], split=True)
  plt.savefig(args.city + '.png', dpi=320)
  print('saved figure to %s.png' % args.city)
