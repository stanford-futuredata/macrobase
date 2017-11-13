package edu.stanford.futuredata.macrobase.analysis.summary.fpg;

import com.google.common.collect.Sets;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.result.FPGItemsetResult;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.result.ItemsetWithCount;

import java.util.*;


public class FPGrowthEmergingGrouped implements FPGrowthAlgorithm {
    private boolean combinationsEnabled = true;

    public FPGrowthEmergingGrouped() {};
    public void setCombinationsEnabled(boolean flag) {
        this.combinationsEnabled = flag;
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


    public List<FPGItemsetResult> getEmergingItemsetsWithMinSupport(
            List<Set<Integer>> inliersRaw,
            List<Set<Integer>> outliersRaw,
            double minSupport,
            double minRatio
    ) {
        GroupByAggregator gb = new GroupByAggregator();
        long startTime = System.currentTimeMillis();
        List<ItemsetWithCount> inlierGrouped = gb.groupBy(inliersRaw);
        List<ItemsetWithCount> outlierGrouped = gb.groupBy(outliersRaw);
        long elapsed = System.currentTimeMillis() - startTime;
        System.out.println("Grouping Time: "+elapsed);
        System.out.println(String.format("Orig: %d/%d After: %d/%d",
                outliersRaw.size(), inliersRaw.size(),
                outlierGrouped.size(), inlierGrouped.size()
        ));
        startTime = System.currentTimeMillis();
        Map<Integer, Double> inlierCounts = new ExactCount().countGrouped(inlierGrouped).getCounts();
        Map<Integer, Double> outlierCounts = new ExactCount().countGrouped(outlierGrouped).getCounts();


        double numOutliers = 0;
        for (ItemsetWithCount ic : outlierGrouped) {
            numOutliers += ic.getCount();
        }
        double numInliers = 0;
        for (ItemsetWithCount ic : inlierGrouped) {
            numInliers += ic.getCount();
        }
        int supportCountRequired = (int) (numOutliers * minSupport);

        Map<Integer, Double> supportedOutlierCounts = new HashMap<>();

        elapsed = System.currentTimeMillis() - startTime;
        System.out.println("Check 1: "+elapsed);
        ArrayList<ItemsetWithCount> outlierTransactionsRaw = new ArrayList<>();
        for (ItemsetWithCount oGroup: outlierGrouped) {
            Set<Integer> txn = null;

            Set<Integer> o = oGroup.getItems();
            double x = oGroup.getCount();

            for (int i : o) {
                double outlierCount = outlierCounts.get(i);
                if (outlierCount >= supportCountRequired) {
                    Number inlierCount = inlierCounts.get(i);

                    double outlierInlierRatio = RiskRatio.compute(inlierCount,
                                                                  outlierCount,
                                                                  numInliers,
                                                                  numOutliers);

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
                outlierTransactionsRaw.add(new ItemsetWithCount(txn,x));
            }
        }
        elapsed = System.currentTimeMillis() - startTime;
        System.out.println("Check 2: "+elapsed);

//        List<ItemsetWithCount> outlierTransactions = gb.groupByWithCounts(outlierTransactionsRaw);
        List<ItemsetWithCount> outlierTransactions = outlierTransactionsRaw;

        FPGrowthGrouped fpg = new FPGrowthGrouped();
        List<ItemsetWithCount> iwc = fpg.getItemsetsWithSupportCount(
                outlierTransactions,
                supportedOutlierCounts,
                numOutliers * minSupport);

        iwc.sort((x, y) -> x.getCount() != y.getCount() ?
                -Double.compare(x.getCount(), y.getCount()) :
                -Double.compare(x.getItems().size(), y.getItems().size()));

        Set<Integer> ratioItemsToCheck = new HashSet<>();
        List<ItemsetWithCount> ratioSetsToCheck = new ArrayList<>();
        List<FPGItemsetResult> ret = new ArrayList<>();

        Set<Integer> prevSet = null;
        Double prevCount = -1.;

        elapsed = System.currentTimeMillis() - startTime;
        System.out.println("Check 3: "+elapsed);
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
                                          numInliers,
                                          numOutliers);

                ret.add(new FPGItemsetResult(i.getCount() / numOutliers,
                                          i.getCount(),
                                          ratio,
                                          i.getItems()));
            } else {
                ratioItemsToCheck.addAll(i.getItems());
                ratioSetsToCheck.add(i);
            }
        }

        elapsed = System.currentTimeMillis() - startTime;
        System.out.println("Check 4: "+elapsed);
        // check the ratios of any itemsets we just marked
        FPGrowthGrouped inlierTree = new FPGrowthGrouped();
        List<ItemsetWithCount> matchingInlierCounts = inlierTree.getCounts(inlierGrouped,
                                                                           inlierCounts,
                                                                           ratioItemsToCheck,
                                                                           ratioSetsToCheck);

        assert (matchingInlierCounts.size() == ratioSetsToCheck.size());
        for (int i = 0; i < matchingInlierCounts.size(); ++i) {
            ItemsetWithCount ic = matchingInlierCounts.get(i);
            ItemsetWithCount oc = ratioSetsToCheck.get(i);

            double ratio = RiskRatio.compute(ic.getCount(),
                                             oc.getCount(),
                                             numInliers,
                                             numOutliers);

            if (ratio >= minRatio) {
                ret.add(new FPGItemsetResult(oc.getCount() / numOutliers,
                                          oc.getCount(),
                                          ratio,
                                          oc.getItems()));
            }
        }
        elapsed = System.currentTimeMillis() - startTime;
        System.out.println("Check 5: "+elapsed);

        // finally sort one last time
        ret.sort((x, y) -> x.getNumRecords() != y.getNumRecords() ?
                -Double.compare(x.getNumRecords(), y.getNumRecords()) :
                -Double.compare(x.getItems().size(), y.getItems().size()));

        elapsed = System.currentTimeMillis() - startTime;
        System.out.println("Check 6: "+elapsed);
        return ret;
    }
}
