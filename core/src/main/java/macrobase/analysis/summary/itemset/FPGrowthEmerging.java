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

    private final boolean combinationsEnabled;

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(FPGrowthEmerging.class);

    public FPGrowthEmerging(boolean combinationsEnabled) {
        this.combinationsEnabled = combinationsEnabled;
    }

    private List<ItemsetResult> getSingletonItemsets(List<Datum> inliers,
                                                     List<Datum> outliers,
                                                     double minSupport,
                                                     double minRatio,
                                                     DatumEncoder encoder) {
        int supportCountRequired = (int) (outliers.size() * minSupport);

        List<ItemsetResult> ret = new ArrayList<>();

        Map<Integer, Double> inlierCounts = new ExactCount().count(inliers).getCounts();
        Map<Integer, Double> outlierCounts = new ExactCount().count(outliers).getCounts();

        for (Map.Entry<Integer, Double> attrOutlierCountEntry : outlierCounts.entrySet()) {
            if (attrOutlierCountEntry.getValue() < supportCountRequired) {
                continue;
            }

            Double attrInlierCount = inlierCounts.get(attrOutlierCountEntry.getKey());

            double ratio = RiskRatio.compute(attrInlierCount,
                                          attrOutlierCountEntry.getValue(),
                                          inliers.size(),
                                          outliers.size());

            if (ratio > minRatio) {
                ret.add(new ItemsetResult(attrOutlierCountEntry.getValue() / outliers.size(),
                                          attrOutlierCountEntry.getValue(),
                                          ratio,
                                          encoder.getColsFromAttr(attrOutlierCountEntry.getKey())));
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
        if (!combinationsEnabled || (inliers.size() > 0 && inliers.get(0).attributes().size() == 1)) {
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

            for (int i : d.attributes()) {
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
                Number inlierCount = inlierCounts.get(i.getItems().iterator().next());

                double ratio = RiskRatio.compute(inlierCount,
                                          i.getCount(),
                                          inliers.size(),
                                          outliers.size());

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

            double ratio = RiskRatio.compute(ic.getCount(),
                                             oc.getCount(),
                                             inliers.size(),
                                             outliers.size());

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
