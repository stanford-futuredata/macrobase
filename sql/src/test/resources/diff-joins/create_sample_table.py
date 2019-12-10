#! /usr/bin/env python

from __future__ import print_function
import pandas as pd
import numpy as np
import argparse


def generate_csv(num_rows, num_cols, num_distinct_vals, fname):
    cols = [str('A' + str(i)) for i in range(num_cols)]

    data = []
    if type(num_distinct_vals) is list or type(num_distinct_vals) is tuple:
        for i in range(num_rows):
            vals = [i]  # first column is the primary key
            vals += list(
                np.random.choice(num_distinct_vals[j])
                for j in range(0, num_cols - 1))
            data.append(vals)
    else:
        for i in range(num_rows):
            vals = (np.random.choice(num_distinct_vals)
                    for j in range(num_cols))
            data.append(vals)

    df = pd.DataFrame(data=data, columns=cols)
    df.to_csv(fname, index=False, header=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Generate sample tables to test joins.')
    parser.add_argument('--num-rows', '-r', type=int, default=100)
    parser.add_argument('--num-cols', '-c', type=int, required=True)
    parser.add_argument('--num-distinct-vals', '-d', type=int, required=True)
    args = parser.parse_args()

    num_rows = args.num_rows
    num_cols = args.num_cols
    num_distinct_vals = args.num_distinct_vals

    generate_csv(num_rows, 1, num_distinct_vals, 'table_R.csv')
    generate_csv(num_rows, 1, num_distinct_vals, 'table_S.csv')
    # add one, to include primary key column, which has one entry
    # for every possible value. The other columns have fewer distinct
    # values
    generate_csv(num_distinct_vals, num_cols + 1, list(int(num_distinct_vals / 5)
                                                   for i in range(num_cols)),
                 'table_T.csv')
