#! /usr/bin/env python


# Generate CSV data with outliers in specific order

"A1,B2"

import argparse
import numpy as np
from itertools import product

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--cardinalities', required=True)
    args = parser.parse_args()

    np.random.randint
    cardinalities = [int(x) for x in args.cardinalities.strip().split(',')]
    outlier_indices = [np.random.randint(val) for val in cardinalities]
    print('Outlier: ' + str(outlier_indices))
    cols = [chr(i + ord('A')) for i in range(len(cardinalities))]
    for value in product(*[range(x) for x in cardinalities]):
        print(value)

    
    

if __name__ == '__main__':
    main()
