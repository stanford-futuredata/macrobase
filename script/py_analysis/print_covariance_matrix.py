import pandas
import argparse

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('csvfile')
  args = parser.parse_args()
  with open(args.csvfile, 'r') as infile:
    frame = pandas.read_csv(infile)
    print frame.cov()
