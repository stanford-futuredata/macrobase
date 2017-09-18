import numpy as np
from timeit import default_timer as timer
from sklearn.linear_model import LogisticRegression
from sklearn.svm import LinearSVC
from sklearn.feature_selection import SelectFromModel
from sklearn.feature_selection import SelectPercentile, f_classif, chi2, mutual_info_classif
from sklearn.feature_selection import RFECV
from sklearn.ensemble import ExtraTreesClassifier
import sys
from collections import Counter
from scipy import special
import operator

from sklearn.utils import (as_float_array, check_X_y, safe_sqr,
                           safe_mask)


class MultiMAD:
    CUTOFF = 2.33  # analogous to z-score
    MIN_SUPPORT = 0.005
    MIN_RR = 3

    MAD_TO_ZSCORE_COEFFICIENT = 1.4826  # do not change

    def __init__(self, use_percentile=True):
        self.classification = []
        self.score_time = 0
        self.low_cutoff = 0
        self.high_cutoff = 0
        self.detect_low = True
        self.detect_high = True
        self.explanations = []
        self.top_ratio_by_metric = []  # top risk ratio from order-1 explanations produced by each metric
        self.num_expl_by_metric = []  # number of order-1 explanations produced by each metric
        self.explanations_by_metric = []  # the order-1 explanations produced by each metric
        self.percentile = use_percentile

    def process(self, input):
        self.classification = []
        for col in range(input.shape[1]):
            metrics = input[:, col]
            if self.percentile:
                self.compute_percentile(metrics)
            else:
                self.compute_mad(metrics)
            start = timer()
            results = self.score(metrics)
            self.score_time += (timer() - start)
            self.classification.append(results)

    def compute_explanation(self, attributes):
        attribute_counts = dict()
        for attr in attributes:
            if attr in attribute_counts:
                attribute_counts[attr] += 1
            else:
                attribute_counts[attr] = 1
        # print(sorted(attribute_counts.items(), key=operator.itemgetter(1)))

        self.top_ratio_by_metric = []
        self.num_expl_by_metric = []
        self.explanations_by_metric = []
        self.explanations = []
        col_no = 0
        for column in self.classification:
            # print "new metric"
            num_outliers = 0
            outlier_counts = dict()
            for i, result in enumerate(column):
                if result == 1:
                    num_outliers += 1
                    attr = attributes[i]
                    if attr in outlier_counts:
                        outlier_counts[attr] += 1
                    else:
                        outlier_counts[attr] = 1
            explanations = []
            support_required = int(MultiMAD.MIN_SUPPORT * num_outliers)
            num_explanations = 0
            top_ratio = 0
            # print("Col {}: Total outliers: {}".format(col_no, num_outliers))
            col_no += 1
            for key, count in outlier_counts.items():
                # if count < support_required:
                # 	continue
                totalExposedCount = attribute_counts[key]
                totalMinusExposedCount = len(column) - attribute_counts[key]
                unexposedOutlierCount = num_outliers - count
                if totalExposedCount == 0:
                    risk_ratio = 0
                elif totalMinusExposedCount == 0:
                    risk_ratio = 0
                elif unexposedOutlierCount == 0:
                    risk_ratio = sys.maxsize
                else:
                    risk_ratio = ((float(count) / float(totalExposedCount)) /
                                  (float(unexposedOutlierCount) / float(totalMinusExposedCount)))
                # print("{}: {}, {}".format(key, risk_ratio, float(count) / num_outliers))
                if count < support_required:
                    continue
                if risk_ratio > top_ratio:
                    top_ratio = risk_ratio
                if risk_ratio < MultiMAD.MIN_RR:
                    continue
                self.explanations.append(Explanation(key, risk_ratio))
                explanations.append(key)
                num_explanations += 1
            self.num_expl_by_metric.append(num_explanations)
            self.top_ratio_by_metric.append(top_ratio)
            self.explanations_by_metric.append(explanations)

    def compute_mad(self, metrics):
        median = np.median(metrics)
        residuals = np.abs(metrics - median)
        MAD = np.median(residuals)
        if MAD == 0:
            # print "MAD is zero, using trimmed means"
            residuals = np.sort(residuals)
            length = len(residuals)
            MAD = np.sum(residuals[int(0.05 * length):int(-0.05 * length)]) / (0.9 * length)
        # print "median: {}, MAD: {}".format(median, MAD)
        MAD *= (MultiMAD.MAD_TO_ZSCORE_COEFFICIENT * MultiMAD.CUTOFF)
        # print "max: {}".format(np.amax(metrics))
        self.low_cutoff = median - MAD
        self.high_cutoff = median + MAD

    def compute_percentile(self, metrics):
        self.low_cutoff = np.percentile(metrics, 1)
        self.high_cutoff = np.percentile(metrics, 99)

    def score(self, metrics):
        results = []
        if self.detect_high and self.detect_low:
            results = np.logical_or(metrics > self.high_cutoff, metrics < self.low_cutoff)
        elif self.detect_low:
            results = metrics < self.low_cutoff
        elif self.detect_high:
            results = metrics > self.high_cutoff
        # print "High cutoff: {}".format(self.high_cutoff)
        return results


class Explanation():
    def __init__(self, key, risk_ratio):
        self.key = key
        self.risk_ratio = risk_ratio


class UnivariateSelector:
    def __init__(self):
        self.scores = None

    def select_features(self, X, y, function):
        selector = SelectPercentile(eval(function), percentile=50)
        selector.fit(X, y)
        self.scores = selector.scores_


def anova(X, y):
    X, y = check_X_y(X, y, ['csr', 'csc', 'coo'])
    args = [X[safe_mask(X, y == k)] for k in np.unique(y)]
    n_classes = len(args)
    args = [as_float_array(a) for a in args]
    n_samples_per_class = np.array([a.shape[0] for a in args])
    n_samples = np.sum(n_samples_per_class)
    ss_alldata = sum(safe_sqr(a).sum(axis=0) for a in args)
    sums_args = [np.asarray(a.sum(axis=0)) for a in args]
    square_of_sums_alldata = sum(sums_args) ** 2
    square_of_sums_args = [s ** 2 for s in sums_args]
    sstot = ss_alldata - square_of_sums_alldata / float(n_samples)
    ssbn = 0.
    for k, _ in enumerate(args):
        ssbn += square_of_sums_args[k] / n_samples_per_class[k]
    ssbn -= square_of_sums_alldata / float(n_samples)
    sswn = sstot - ssbn
    dfbn = n_classes - 1
    dfwn = n_samples - n_classes
    msb = ssbn / float(dfbn)
    msw = sswn / float(dfwn)
    constant_features_idx = np.where(msw == 0.)[0]
    # if (np.nonzero(msb)[0].size != msb.size and constant_features_idx.size):
    # 	warnings.warn("Features %s are constant." % constant_features_idx,
    # 				  UserWarning)
    f = msb / msw
    for i in range(len(msb)):
        if msb[i] < 1e-8 and msw[i] < 1e-8:
            f[i] = 0.0
    # flatten matrix to vector in sparse case
    f = np.asarray(f).ravel()
    prob = special.fdtrc(dfbn, dfwn, f)
    return f, prob


class ModelSelector:
    def __init__(self):
        self.coef = None
        self.model = None

    def select_features_svm(self, X, y):
        lsvc = LinearSVC(C=0.0002, penalty="l1", dual=False).fit(X, y)
        self.coef = np.amax(np.abs(lsvc.coef_), axis=0)
        self.model = SelectFromModel(lsvc, prefit=True)

    def select_features_lr(self, X, y):
        lr = LogisticRegression(C=0.001, penalty="l1", dual=False).fit(X, y)
        self.coef = np.amax(np.abs(lr.coef_), axis=0)
        self.model = SelectFromModel(lr, prefit=True)

    def select_features_tree(self, X, y):
        clf = ExtraTreesClassifier().fit(X, y)
        self.model = SelectFromModel(clf, prefit=True)
        self.coef = clf.feature_importances_


class RecursiveSelector:
    def __init__(self):
        self.selector = None

    def select_features(self, X, y):
        estimator = LogisticRegression(C=0.01, penalty="l1", dual=False)
        selector = RFECV(estimator, step=0.5)
        self.selector = selector.fit(X, y)
