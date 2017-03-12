package macrobase.analysis.summary.itemset;

import com.google.common.collect.Sets;
import macrobase.analysis.summary.itemset.result.EncodedItemsetResult;
import macrobase.analysis.summary.itemset.result.ItemsetWithCount;
import macrobase.analysis.summary.count.ExactCount;
import macrobase.analysis.summary.itemset.result.ItemsetResult;

import java.util.*;


public class FPGrowthEmerging {
    private boolean combinationsEnabled = true;

    public FPGrowthEmerging() {};
    public void setCombinationsEnabled(boolean combinationsEnabled) {
        this.combinationsEnabled = combinationsEnabled;
    }


    private List<EncodedItemsetResult> getSingletonItemsets(List<Set<Integer>> inliers,
                                                            List<Set<Integer>> outliers,
                                                            double minSupport,
                                                            double minRatio) {
        int supportCountRequired = (int) (outliers.size() * minSupport);

        List<EncodedItemsetResult> ret = new ArrayList<>();

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
                ret.add(new EncodedItemsetResult(attrOutlierCountEntry.getValue() / outliers.size(),
                                          attrOutlierCountEntry.getValue(),
                                          ratio,
                                          new HashSet<>(item)));
            }
        }

        return ret;
    }

    public List<EncodedItemsetResult> getEmergingItemsetsWithMinSupport(List<Set<Integer>> inliers,
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
        List<ItemsetWithCount> iwc = fpg.getItemsetsWithSupportCount(outlierTransactions, supportedOutlierCounts,
                                                                     outliers.size() * minSupport);

        iwc.sort((x, y) -> x.getCount() != y.getCount() ?
                -Double.compare(x.getCount(), y.getCount()) :
                -Double.compare(x.getItems().size(), y.getItems().size()));

        Set<Integer> ratioItemsToCheck = new HashSet<>();
        List<ItemsetWithCount> ratioSetsToCheck = new ArrayList<>();
        List<EncodedItemsetResult> ret = new ArrayList<>();

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

                ret.add(new EncodedItemsetResult(i.getCount() / (double) outliers.size(),
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
        //int newSize = Math.min(inliers.size(), outliers.size()*100);
        //log.debug("Truncating inliers (size {}) to size {} (outlier size: {})",
        //          inliers.size(), newSize, outliers.size());
        //inliers = inliers.subList(0, newSize);
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
                ret.add(new EncodedItemsetResult(oc.getCount() / (double) outliers.size(),
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
