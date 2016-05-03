import argparse
import json
import matplotlib.pyplot as plt
import numpy as np
import os

def plot_one_context(fileName, context):
    f = open(fileName,'r')
    lines = f.readlines()
    arrayLines = np.array(lines).astype(np.float)
    plt.hist(arrayLines)
    plt.title(context)
    plt.savefig(fileName + "_plot.pdf", bbox_inches='tight')
    plt.close()
    
def plot_context_dir(dir):
    onlyfiles = [f for f in os.listdir(dir) if os.path.isfile(os.path.join(dir, f)) and f.endswith('.txt') and ('outliers' not in f) and  ('inliners' not in f)]    
    for f in onlyfiles:
        context = open(os.path.join(dir, f),'r').readline()
        fOutliers = os.path.join(dir, f) + '_outliers.txt'
        fInliners = os.path.join(dir, f) + '_inliners.txt'
        if(os.path.exists(fOutliers) and os.path.exists(fInliners)):
            plot_one_context(fInliners,context + "   Inliners")
            plot_one_context(fOutliers,context + "   Outliers")
        #plot_one_context(os.path.join(dir, f))
 
if __name__ == '__main__':
    plot_context_dir('/Users/xuchu/Desktop/contextual/workflows/cmt1000000/05-10-10:43:51')