Analyzing Multivariate Datasets
=========

In this tutorial we will examine datasets with multiple metrics using Macrobase. 
There are a variety of outlier detectors available, each of which work best 
on different kinds of data: deciding which to use and configuring them is
important for getting good results. Macrobase currently supports
a number of robust Gaussian estimators as well as a nonparametric Kernel Density
Estimator. Choosing which estimator to use, and which target percentile to set them to,
are the two most important parameters.

## Simple Distributions
We start by examining a simple distribution, and can run a script to 
generate samples from a multivariate gaussian with uniform noise:

    python3 tools/gen_data/gen_multivariate_gaussian.py

This should save some test data in ```target/mv_gaussian_noise.csv```

![Gaussian with Noise](mvgauss.png)


The majority of the data comes has attribute A and is generated from a 2-d gaussian
distribution, while 5% of the data has attribute B and comes from a 2-d uniform distribution
that overlaps with 

### Gaussian Models

    ./bin/batch.sh tools/demos/multivariate_gaussian/mad.yaml
   
[[1d distribution]]
    
    ./bin/batch.sh tools/demos/multivariate_gaussian/rcov.yaml

[[fitted gaussian]]
    
    ./bin/batch.sh tools/demos/multivariate_gaussian/mcd.yaml

[[fitted gaussian]]
    
### Nonparametric Estimation

    ./bin/batch.sh tools/demos/multivariate_gaussian/kde.yaml

[[kde heatmap]]
why does kde do worse?

## A More Complicated Distribution: Shuttle Dataset

[[class distribution]]

### Gaussian Model

[[fitted gaussian + outlier class]]

### Kernel Density Estimation

[[kde contours + outlier class]]
