package edu.stanford.futuredata.macrobase.analysis.summary.fpg;

import com.google.common.collect.Sets;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.result.FPGItemsetResult;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.result.ItemsetWithCount;

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
                                                        double minSupport,
                                                        double minRatio) {
        int supportCountRequired = (int) (outliers.size() * minSupport);

        List<FPGItemsetResult> ret = new ArrayList<>();

        Map<Integer, Double> inlierCounts = new ExactCount().count(inliers).getCounts();
        Map<Integer, Double> outlierCounts = new ExactCount().count(outliers).getCounts();

        for (Map.Entry<Integer, Double> attrOutlierCountEntry : outlierCounts.entrySet()) {
            if (attrOutlierCountEntry.getValue() < supportCountRequired) {
                continue;
            }

            int item = attrOutlierCountEntry.getKey();
            Double attrInlierCount = inlierCounts.get(item);

            double ratio = RiskRatio.compute(attrInlierCount,
                    attrOutlierCountEntry.getValue(),
                    inliers.size(),
                    outliers.size());

            if (ratio > minRatio) {
                ret.add(new FPGItemsetResult(
                        attrOutlierCountEntry.getValue() / outliers.size(),
                        attrOutlierCountEntry.getValue(),
                        ratio,
                        Collections.singleton(item)
                ));
            }
        }
        return ret;
    }

    public List<FPGItemsetResult> getEmergingItemsetsWithMinSupport(List<Set<Integer>> inliers,
                                                                    List<Set<Integer>> outliers,
                                                                    double minSupport,
                                                                    double minRatio) {
        if (!combinationsEnabled || (inliers.size() > 0 && inliers.get(0).size() == 1)) {
            return getSingletonItemsets(inliers, outliers, minSupport, minRatio);
        }

        ArrayList<Set<Integer>> outlierTransactions = new ArrayList<>();

        Map<Integer, Double> inlierCounts = new ExactCount().count(inliers).getCounts();
        Map<Integer, Double> outlierCounts = new ExactCount().count(outliers).getCounts();

        Map<Integer, Double> supportedOutlierCounts = new HashMap<>();

        int supportCountRequired = (int) (outliers.size() * minSupport);

        for (Set<Integer> o: outliers) {
            Set<Integer> txn = null;

            for (int i : o) {
                double outlierCount = outlierCounts.get(i);
                if (outlierCount >= supportCountRequired) {
                    Number inlierCount = inlierCounts.get(i);

                    double outlierInlierRatio = RiskRatio.compute(inlierCount,
                            outlierCount,
                            inliers.size(),
                            outliers.size());

                    if (outlierInlierRatio > minRatio) {
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

            double ratio = RiskRatio.compute(ic.getCount(),
                    oc.getCount(),
                    inliers.size(),
                    outliers.size());

            if (ratio >= minRatio) {
                ret.add(new FPGItemsetResult(oc.getCount() / (double) outliers.size(),
                        oc.getCount(),
                        ratio,
                        oc.getItems()));
            }
        }


        // finally sort one last time
        ret.sort((x, y) -> x.getNumRecords() != y.getNumRecords() ?
                -Double.compare(x.getNumRecords(), y.getNumRecords()) :
                -Double.compare(x.getItems().size(), y.getItems().size()));

        return ret;
    }
}
