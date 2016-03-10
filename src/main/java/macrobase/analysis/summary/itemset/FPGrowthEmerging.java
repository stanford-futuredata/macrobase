package macrobase.analysis.summary.itemset;

import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.common.collect.Sets;
import macrobase.MacroBase;
import macrobase.analysis.summary.count.ExactCount;
import macrobase.analysis.summary.itemset.result.ItemsetResult;
import macrobase.analysis.summary.itemset.result.ItemsetWithCount;
import macrobase.datamodel.Datum;
import macrobase.ingest.DatumEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.codahale.metrics.MetricRegistry.name;

public class FPGrowthEmerging {
    private final Timer singleItemCounts = MacroBase.metrics.timer(name(FPGrowthEmerging.class, "singleItemCounts"));
    private final Timer outlierFPGrowth = MacroBase.metrics.timer(name(FPGrowthEmerging.class, "outlierFPGrowth"));
    private final Timer inlierRatio = MacroBase.metrics.timer(name(FPGrowthEmerging.class, "inlierRatio"));


    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(FPGrowthEmerging.class);

    private List<ItemsetResult> getSingletonItemsets(List<Datum> inliers,
                                                     List<Datum> outliers,
                                                     double minSupport,
                                                     double minRatio,
                                                     DatumEncoder encoder) {
        int supportCountRequired = (int) (outliers.size() * minSupport);

        List<ItemsetResult> ret = new ArrayList<>();

        Map<Integer, Double> inlierCounts = new ExactCount().count(inliers).getCounts();
        Map<Integer, Double> outlierCounts = new ExactCount().count(outliers).getCounts();

        for (Map.Entry<Integer, Double> outlierCount : outlierCounts.entrySet()) {
            if (outlierCount.getValue() < supportCountRequired) {
                continue;
            }

            Double inlierCount = inlierCounts.get(outlierCount.getKey());

            double ratio;

            if (inlierCount != null) {
                ratio = (outlierCount.getValue() / outliers.size()) /
                        (inlierCount / inliers.size());
            } else {
                ratio = Double.POSITIVE_INFINITY;
            }

            if (ratio > minRatio) {
                ret.add(new ItemsetResult(outlierCount.getValue() / outliers.size(),
                                          outlierCount.getValue(),
                                          ratio,
                                          encoder.getColsFromAttr(outlierCount.getKey())));
            }
        }

        return ret;
    }

    public List<ItemsetResult> getEmergingItemsetsWithMinSupport(List<Datum> inliers,
                                                                 List<Datum> outliers,
                                                                 double minSupport,
                                                                 double minRatio,
                                                                 // would prefer not to pass this in, but easier for now...
                                                                 DatumEncoder encoder) {
        if (inliers.get(0).getAttributes().size() == 1) {
            return getSingletonItemsets(inliers, outliers, minSupport, minRatio, encoder);
        }

        Context context = singleItemCounts.time();
        ArrayList<Set<Integer>> outlierTransactions = new ArrayList<>();

        Map<Integer, Double> inlierCounts = new ExactCount().count(inliers).getCounts();
        Map<Integer, Double> outlierCounts = new ExactCount().count(outliers).getCounts();


        Map<Integer, Double> supportedOutlierCounts = new HashMap<>();

        int supportCountRequired = (int) (outliers.size() * minSupport);

        for (Datum d : outliers) {
            Set<Integer> txn = null;

            for (int i : d.getAttributes()) {
                double outlierCount = outlierCounts.get(i);
                if (outlierCount >= supportCountRequired) {
                    Number inlierCount = inlierCounts.get(i);

                    double outlierInlierRatio;
                    if (inlierCount == null || inlierCount.doubleValue() == 0) {
                        outlierInlierRatio = Double.POSITIVE_INFINITY;
                    } else {
                        outlierInlierRatio = (outlierCount / outliers.size()) / (inlierCount.doubleValue() / inliers.size());
                    }
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
        context.stop();

        context = outlierFPGrowth.time();
        FPGrowth fpg = new FPGrowth();
        List<ItemsetWithCount> iwc = fpg.getItemsetsWithSupportCount(outlierTransactions, supportedOutlierCounts,
                                                                     outliers.size() * minSupport);
        context.stop();

        context = inlierRatio.time();

        iwc.sort((x, y) -> x.getCount() != y.getCount() ?
                -Double.compare(x.getCount(), y.getCount()) :
                -Double.compare(x.getItems().size(), y.getItems().size()));

        Set<Integer> ratioItemsToCheck = new HashSet<>();
        List<ItemsetWithCount> ratioSetsToCheck = new ArrayList<>();
        List<ItemsetResult> ret = new ArrayList<>();

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
                double ratio = 0;
                Number inlierCount = inlierCounts.get(i.getItems().iterator().next());

                if (inlierCount != null && inlierCount.doubleValue() > 0) {
                    ratio = (i.getCount() / outliers.size()) / ((double) inlierCount / inliers.size());
                } else {
                    ratio = Double.POSITIVE_INFINITY;
                }

                ret.add(new ItemsetResult(i.getCount() / (double) outliers.size(),
                                          i.getCount(),
                                          ratio,
                                          encoder.getColsFromAttrSet(i.getItems())));
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

            double ratio;
            if (ic.getCount() > 0) {
                ratio = (oc.getCount() / outliers.size()) / (ic.getCount() / inliers.size());
            } else {
                ratio = Double.POSITIVE_INFINITY;
            }

            if (ratio >= minRatio) {
                ret.add(new ItemsetResult(oc.getCount() / (double) outliers.size(),
                                          oc.getCount(),
                                          ratio,
                                          encoder.getColsFromAttrSet(oc.getItems())));
            }
        }

        context.stop();

        // finally sort one last time
        ret.sort((x, y) -> x.getNumRecords() != y.getNumRecords() ?
                -Double.compare(x.getNumRecords(), y.getNumRecords()) :
                -Double.compare(x.getItems().size(), y.getItems().size()));

        return ret;
    }
}
