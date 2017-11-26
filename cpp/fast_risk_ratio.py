#import hyperloglog
import pandas as pd
from collections import defaultdict
import itertools
from streamlib import CountMin


ATTR_COLS = ['location', 'version', 'name']
METRIC_COLS = ['usage', 'latency']
THRESHOLD = 0.1
INT_SIZE = 32


def generate_candidate_col(df):
    num_rows = len(df)
    candidate_vals_for_cols = {}

    for col in ATTR_COLS:
        candidate_vals_for_cols[col] = {}
        cm = CountMin()
        vals = set(df[col])
        cm.processBatch(df[col])
        for val in vals:
            est = cm.estimate(val)
            if (1.0*est/num_rows >= THRESHOLD):
                candidate_vals_for_cols[col][val] = est

    valid_columns = []
    for col in candidate_vals_for_cols.keys():
        print "column:", col
        l = candidate_vals_for_cols[col]
        if len(l) == 0:
            continue
        valid_columns.append(col)
        for val, cnt in l.iteritems():
            print "value:", val, "count:", cnt
    valid_columns.sort()
    return valid_columns, candidate_vals_for_cols
    
def get_pairs(df, valid_columns, candidate_vals_for_cols):
    cmsPairs = {}
    for combo in itertools.combinations(valid_columns, 2):
        cmsPairs[combo] = CountMin()
    for _, row in df.iterrows():
        for combo in itertools.combinations(valid_columns, 2):
            a, b = combo 
            if row[a] in candidate_vals_for_cols[a] and row[b] in candidate_vals_for_cols[b]:
                cmsPairs[combo].processItem((row[a], row[b]))

    for combo in itertools.combinations(valid_columns, 2):
        a, b = combo
        for aval, bval in itertools.product(candidate_vals_for_cols[a].keys(), candidate_vals_for_cols[b].keys()):
            print aval, bval, "count:", cmsPairs[combo].estimate((aval, bval))

def get_triples(df, valid_columns, candidate_vals_for_cols):
    cmsTriples = {}
    for combo in itertools.combinations(valid_columns, 3):
        cmsTriples[combo] = CountMin()
    for _, row in df.iterrows():
        for combo in itertools.combinations(valid_columns, 3):
            a, b, c = combo 
            if row[a] in candidate_vals_for_cols[a] and row[b] in candidate_vals_for_cols[b] and row[c] in candidate_vals_for_cols[c]:
                cmsTriples[combo].processItem((row[a], row[b], row[c]))

    for combo in itertools.combinations(valid_columns, 3):
        a, b, c = combo
        for aval, bval, cval in itertools.product(candidate_vals_for_cols[a].keys(), candidate_vals_for_cols[b].keys(), candidate_vals_for_cols[c].keys()):
            print aval, bval, cval, "count:", cmsTriples[combo].estimate((aval, bval, cval))

def generate_bitvectors(df, attr_cols=ATTR_COLS):
    bitvector_dict = {}
    for col in attr_cols:
        bitvector_dict[col] = defaultdict(set)
    for i, row in df.iterrows():
        for col in attr_cols:
            bitvector_dict[col][row[col]].add(i)
    return bitvector_dict

def main():
    df = pd.read_csv('../core/demo/generated_sample.csv')
    #df = pd.read_csv('../core/demo/sample.csv')
    
    bitvectors = generate_bitvectors(df)
    for col in ATTR_COLS:
        for key in bitvectors[col].keys():
            print ('Cardinality', '%s' % key, len(bitvectors[col][key]))

    for combo in itertools.combinations(ATTR_COLS, 2):
        a, b = combo
        bitmapa = bitvectors[a]
        bitmapb = bitvectors[b]
        for keya, keyb in itertools.product(bitmapa.keys(), bitmapb.keys()):
            bitmaska = bitmapa[keya]
            bitmaskb = bitmapb[keyb]
            combined = bitmaska.intersection(bitmaskb)
            print('Combined cardinality', '(%s, %s)' % (keya, keyb), len(combined))
            #print('Location cardinality of', location_key, len(bits_for_loc_key))
            #print('Version cardinality of', version_key, len(bits_for_version_key))
    
    for combo in itertools.combinations(ATTR_COLS, 3):
        a, b, c = combo
        bitmapa = bitvectors[a]
        bitmapb = bitvectors[b]
        bitmapc = bitvectors[c]
        for keya, keyb, keyc in itertools.product(bitmapa.keys(), bitmapb.keys(), bitmapc.keys()):
            bitmaska = bitmapa[keya]
            bitmaskb = bitmapb[keyb]
            bitmaskc = bitmapc[keyc]
            combined = bitmaska.intersection(bitmaskb.intersection(bitmaskc))
            print('Combined cardinality', '(%s, %s, %s)' % (keya, keyb, keyc), len(combined))

    '''
    valid_columns, candidate_vals_for_cols = generate_candidate_col(df)
    get_pairs(df, valid_columns, candidate_vals_for_cols)
    get_triples(df, valid_columns, candidate_vals_for_cols)
    '''

if __name__ == '__main__':
    main()

# hypothesis: NC3 candidate generation is much slower than bitvectors
#true_card = defaultdict(int)
##hll = hyperloglog.HyperLogLog(0.01)
#for _, row in df.iterrows():
##    hll.add(row['location'])
#    true_card[row['location']] += 1

#print "estimate:", len(hll)
#print "cardinality upper bound:", len(df)/len(hll)
#print "true:", len(true_card)
#print "max true card:", max(true_card.values())

# Problem: Need to estimate upper bound of the max cardinality of any value in a column

#import sys
#sys.path.append("StreamLib")
#from streamlib import CountMin
#
#
#for col in dr.itercols():
#    cm = CountMin()
#    print col
#    cm.processBatch(col)
#    for key, cnt in true_card.iteritems():
#        print 'Estimated frequency of', key, 'is', cm.estimate(key), 'true count is', cnt

#run hyperloglog on both outlier and inlier table
#Outliers table = Out
#Inliers table = In
##for outliers:
#for col in cols:
#    N_distinct = hyperloglog(col)
#    If len(col) / n_distinct > OUTLIER_THRESHOLD: # define on a per-query basis (e.g. "WHERE support > 0.5")
#        candidate_cols.append(col)
##for inliers
#for col in cols:
#    N_distinct = hyperloglog(col)
#    If len(col) / n_distinct < INLIER_THRESHOLD: # define on a per-query basis (e.g. "WHERE support > 0.5")
#        candidate_cols.append(col)
#
#    for row in Out:
#        Update the counts for all combinations NC1 + NC2 + NC3
#        For inliers in In:
#            Bitvector of each attribute v
#
#            For each col in cols:
#                For each val in distinct(col):
#                    Generate bitvector of indices for that value in the col
# number of bitvectors is O(n_distinct*num_col)
# length of bitvector is num_rows
# bitvector can't really be compressed because we expect to see many 1s
# possibly tqke advantage of skew to compress subregions of the bitvectors
