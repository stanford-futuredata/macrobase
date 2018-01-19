package edu.stanford.futuredata.macrobase.analysis.summary.fpg;

import com.google.common.collect.Sets;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.result.FPGItemsetResult;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.result.ItemsetWithCount;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.QualityMetric;

import java.util.*;


public class FPGrowthEmerging {
    private boolean combinationsEnabled = true;

    public FPGrowthEmerging() {};
    public FPGrowthEmerging setCombinationsEnabled(boolean flag) {
        this.combinationsEnabled = flag;
        return this;
    }


    private List<FPGItemsetResult> getSingletonItemsets(List<Set<Integer>> inliers,
                                                        List<Set<Integer>> outliers,
                                                        List<QualityMetric> qualityMetrics,
                                                        List<Double> thresholds,
                                                        double minSupport) {
        int supportCountRequired = (int) (outliers.size() * minSupport);

        List<FPGItemsetResult> ret = new ArrayList<>();

        // Count total occurences of each attribute in both inliers and outliers
        Map<Integer, Double> inlierCounts = new ExactCount().count(inliers).getCounts();
        Map<Integer, Double> outlierCounts = new ExactCount().count(outliers).getCounts();

        for (Map.Entry<Integer, Double> attrOutlierCountEntry : outlierCounts.entrySet()) {
            if (attrOutlierCountEntry.getValue() < supportCountRequired) {
                continue;
            }

            int item = attrOutlierCountEntry.getKey();
            Double attrInlierCount = inlierCounts.get(item);

            boolean passThresholds = true;
            for(int q = 0; q < qualityMetrics.size(); q++) {
                double[] curAggregate = new double[]{
                        attrOutlierCountEntry.getValue(),
                        attrOutlierCountEntry.getValue() + attrInlierCount};
                double metric = qualityMetrics.get(q).value(curAggregate);
                if (!(metric > thresholds.get(q))) {
                    passThresholds = false;
                }
            }

            if (passThresholds) {
                double riskRatio = RiskRatio.compute(attrInlierCount,
                        attrOutlierCountEntry.getValue(),
                        inliers.size(),
                        outliers.size());
                ret.add(new FPGItemsetResult(
                        attrOutlierCountEntry.getValue() / outliers.size(),
                        attrOutlierCountEntry.getValue(),
                        riskRatio,
                        Collections.singleton(item)
                ));
            }
        }
        return ret;
    }

    public List<FPGItemsetResult> getEmergingItemsetsWithMinSupport(List<Set<Integer>> inliers,
                                                                    List<Set<Integer>> outliers,
                                                                    List<QualityMetric> qualityMetrics,
                                                                    List<Double> thresholds,
                                                                    double minSupport) {
        // Initialize the qualityMetrics with the total number of outliers and inliers
        double[] globalAggregates = new double[]{outliers.size(), outliers.size() + inliers.size()};
        for (QualityMetric q : qualityMetrics) {
            q.initialize(globalAggregates);
        }

        if (!combinationsEnabled || (inliers.size() > 0 && inliers.get(0).size() == 1)) {
            return getSingletonItemsets(inliers, outliers, qualityMetrics, thresholds, minSupport);
        }

        ArrayList<Set<Integer>> outlierTransactions = new ArrayList<>();

        // Count total occurences of each attribute in both inliers and outliers
        Map<Integer, Double> inlierCounts = new ExactCount().count(inliers).getCounts();
        Map<Integer, Double> outlierCounts = new ExactCount().count(outliers).getCounts();
        for (int key: outlierCounts.keySet()) {
            if (!inlierCounts.containsKey(key)) {
                inlierCounts.put(key, 0.0);
            }
        }

        Map<Integer, Double> supportedOutlierCounts = new HashMap<>();

        int supportCountRequired = (int) (outliers.size() * minSupport);

        for (Set<Integer> o: outliers) {
            Set<Integer> txn = null;

            for (int i : o) {
                double outlierCount = outlierCounts.get(i);
                if (outlierCount >= supportCountRequired) {

                    boolean passThresholds = true;
                    double[] curAggregate = new double[]{
                            outlierCount,
                            outlierCount + inlierCounts.get(i)
                    };
                    for (int q = 0; q < qualityMetrics.size(); q++) {
                        double metric = qualityMetrics.get(q).value(curAggregate);
                        if (!(metric > thresholds.get(q))) {
                            passThresholds = false;
                        }
                    }

                    if (passThresholds) {
                        if (txn == null) {
                            txn = new HashSet<>();
                        }
                        if (!supportedOutlierCounts.containsKey(i)) {
                            supportedOutlierCounts.put(i, outlierCount);
                        }

                        txn.add(i);
                    }
                }
            }

            if (txn != null) {
                outlierTransactions.add(txn);
            }
        }

        FPGrowth fpg = new FPGrowth();
        List<ItemsetWithCount> iwc = fpg.getItemsetsWithSupportCount(
                outlierTransactions,
                supportedOutlierCounts,
                outliers.size() * minSupport);

        iwc.sort((x, y) -> x.getCount() != y.getCount() ?
                -Double.compare(x.getCount(), y.getCount()) :
                -Double.compare(x.getItems().size(), y.getItems().size()));

        Set<Integer> ratioItemsToCheck = new HashSet<>();
        List<ItemsetWithCount> ratioSetsToCheck = new ArrayList<>();
        List<FPGItemsetResult> ret = new ArrayList<>();

        Set<Integer> prevSet = null;
        Double prevCount = -1.;
        for (ItemsetWithCount i : iwc) {
            if (i.getCount() == prevCount) {
                if (prevSet != null && Sets.difference(i.getItems(), prevSet).size() == 0) {
                    continue;
                }
            }


            prevCount = i.getCount();
            prevSet = i.getItems();

            if (i.getItems().size() == 1) {
                Number inlierCount = inlierCounts.get(i.getItems().iterator().next());

                double ratio = RiskRatio.compute(inlierCount,
                                          i.getCount(),
                                          inliers.size(),
                                          outliers.size());

                ret.add(new FPGItemsetResult(i.getCount() / (double) outliers.size(),
                                          i.getCount(),
                                          ratio,
                                          i.getItems()));
            } else {
                ratioItemsToCheck.addAll(i.getItems());
                ratioSetsToCheck.add(i);
            }
        }

        // check the ratios of any itemsets we just marked
        FPGrowth inlierTree = new FPGrowth();
        List<ItemsetWithCount> matchingInlierCounts = inlierTree.getCounts(inliers,
                                                                           inlierCounts,
                                                                           ratioItemsToCheck,
                                                                           ratioSetsToCheck);

        assert (matchingInlierCounts.size() == ratioSetsToCheck.size());
        for (int i = 0; i < matchingInlierCounts.size(); ++i) {
            ItemsetWithCount ic = matchingInlierCounts.get(i);
            ItemsetWithCount oc = ratioSetsToCheck.get(i);

            boolean passThresholds = true;
            double[] curAggregate = new double[]{
                    oc.getCount(),
                    oc.getCount() + ic.getCount()
            };
            for(int q = 0; q < qualityMetrics.size(); q++) {
                double metric = qualityMetrics.get(q).value(curAggregate);
                if (!(metric > thresholds.get(q))) {
                    passThresholds = false;
                }
            }

            if (passThresholds) {
                double riskRatio = RiskRatio.compute(ic.getCount(),
                        oc.getCount(),
                        inliers.size(),
                        outliers.size());
                ret.add(new FPGItemsetResult(
                        oc.getCount() / (double) outliers.size(),
                        oc.getCount(),
                        riskRatio,
                        oc.getItems()
                ));
            }
        }


        // finally sort one last time
        ret.sort((x, y) -> x.getNumRecords() != y.getNumRecords() ?
                -Double.compare(x.getNumRecords(), y.getNumRecords()) :
                -Double.compare(x.getItems().size(), y.getItems().size()));

        return ret;
    }
}
