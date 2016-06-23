import argparse
import pandas as pd

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--csv', default='target/phillips_data/power_usage_city_state_rtus.csv')
  parser.add_argument('--controller-id', type=int)
  args = parser.parse_args()

  df = pd.read_csv(args.csv)
  x = df[df['controller_id'] == args.controller_id]
  
