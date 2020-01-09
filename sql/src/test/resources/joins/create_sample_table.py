#! /usr/bin/env python

from __future__ import print_function
import pandas as pd
import numpy as np
import argparse


def generate_csv(start_index, fname):
    cols = [
        str('A' + str(i)) for i in range(start_index, NUM_COLS + start_index)
    ]

    data = []
    for i in range(NUM_ROWS):
        vals = (np.random.choice(NUM_DISTINCT_VALS) for j in range(NUM_COLS))
        data.append(vals)

    df = pd.DataFrame(data=data, columns=cols)
    df.to_csv(fname, index=False, header=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Generate sample tables to test joins.')
    parser.add_argument('--num-rows', '-r', type=int, default=100)
    parser.add_argument('--num-cols', '-c', type=int, required=True)
    parser.add_argument('--num-distinct-vals', '-d', type=int, required=True)
    parser.add_argument('--num-cols-overlap', '-o', type=int, default=1)
    args = parser.parse_args()

    NUM_ROWS = args.num_rows
    NUM_COLS = args.num_cols
    NUM_DISTINCT_VALS = args.num_distinct_vals
    num_overlap = args.num_cols_overlap

    if num_overlap > NUM_COLS:
        print('--num-cols-overlap cannot be greater than --num-cols')
        import sys
        sys.exit(1)

    generate_csv(0, 'table_a.csv')
    generate_csv(NUM_COLS - num_overlap, 'table_b.csv')
