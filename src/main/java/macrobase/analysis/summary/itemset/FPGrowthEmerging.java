package macrobase.analysis.summary.itemset;

import com.google.common.collect.Sets;
import macrobase.analysis.outlier.result.DatumWithScore;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.summary.count.ExactCount;
import macrobase.analysis.summary.itemset.result.ItemsetResult;
import macrobase.analysis.summary.itemset.result.ItemsetWithCount;
import macrobase.ingest.DatumEncoder;
import macrobase.ingest.result.ColumnValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class FPGrowthEmerging {
    private static final Logger log = LoggerFactory.getLogger(FPGrowthEmerging.class);

    public List<ItemsetResult> getEmergingItemsetsWithMinSupport(List<DatumWithScore> inliers,
                                                                 List<DatumWithScore> outliers,
                                                                 double minSupport,
                                                                 double minRatio,
                                                                 // would prefer not to pass this in, but easier for now...
                                                                 DatumEncoder encoder) {
        // TODO: truncate inliers!
        ArrayList<Set<Integer>> outlierTransactions = new ArrayList<>();

        Map<Integer, Integer> inlierCounts = new ExactCount().count(inliers).getCounts();
        Map<Integer, Integer> outlierCounts = new ExactCount().count(outliers).getCounts();

        int supportCountRequired = (int)(outliers.size()*minSupport);

        for(DatumWithScore d : outliers) {
            Set<Integer> txn = null;

            for(int i : d.getDatum().getAttributes()) {
                int outlierCount = outlierCounts.get(i);
                if(outlierCount >= supportCountRequired) {
                    Integer inlierCount = inlierCounts.get(i);

                    double outlierInlierRatio;
                    if(inlierCount == null || inlierCount == 0) {
                        outlierInlierRatio = Double.POSITIVE_INFINITY;
                    } else {
                        outlierInlierRatio = ((double)outlierCount/outliers.size())/((double)inlierCount/inliers.size());
                    }
                    if(outlierInlierRatio > minRatio) {
                        if(txn == null) {
                            txn = new HashSet<>();
                        }
                        txn.add(i);
                    }
                }
            }

            if(txn != null) {
                outlierTransactions.add(txn);
            }
        }

        FPGrowth fpg = new FPGrowth();
        List<ItemsetWithCount> iwc = fpg.getItemsets(outlierTransactions, minSupport);

        // to fix!
        final double TEMP_INLIER_RATIO = 0;

        iwc.sort((x, y) -> x.getCount() != y.getCount() ?
                -Integer.compare(x.getCount(), y.getCount()) :
                -Integer.compare(x.getItems().size(), y.getItems().size()));

        List<ItemsetResult> isr = new ArrayList<>();


        Set<Integer> ratioItemsToCheck = new HashSet<>();
        List<ItemsetWithCount> ratioSetsToCheck = new ArrayList<>();
        List<ItemsetResult> ret = new ArrayList<>();

        Set<Integer> prevSet = null;
        Integer prevCount = -1;
        for(ItemsetWithCount i : iwc) {
            if(i.getCount() == prevCount) {
                if(prevSet != null && Sets.difference(i.getItems(), prevSet).size() == 0) {
                    continue;
                }
            }

            prevCount = i.getCount();
            prevSet = i.getItems();


            if(i.getItems().size() == 1) {
                double ratio = 0;
                Integer inlierCount = inlierCounts.get(i.getItems().iterator().next());

                if(inlierCount != null && inlierCount > 0) {
                    ratio = (i.getCount()/outliers.size())/((double)inlierCount/inliers.size());
                } else {
                    ratio = Double.POSITIVE_INFINITY;
                }

                ret.add(new ItemsetResult(i.getCount()/(double)outliers.size(),
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
        List<ItemsetWithCount> matchingInlierCounts = inlierTree.getCounts(inliers,
                                                                           inlierCounts,
                                                                           ratioItemsToCheck,
                                                                           ratioSetsToCheck);

        assert(matchingInlierCounts.size() == ratioSetsToCheck.size());
        for(int i = 0; i < matchingInlierCounts.size(); ++i) {
            ItemsetWithCount ic = matchingInlierCounts.get(i);
            ItemsetWithCount oc = ratioSetsToCheck.get(i);

            double ratio;
            if(ic.getCount() > 0) {
                ratio = (oc.getCount()/outliers.size())/((double)ic.getCount()/inliers.size());
            } else {
                ratio = Double.POSITIVE_INFINITY;
            }

            if(ratio >= minRatio) {
                ret.add(new ItemsetResult(oc.getCount()/(double)outliers.size(),
                                          oc.getCount(),
                                          ratio,
                                          encoder.getColsFromAttrSet(oc.getItems())));
            }
        }

        // finally sort one last time
        ret.sort((x, y) -> x.getNumRecords() != y.getNumRecords() ?
                -Integer.compare(x.getNumRecords(), y.getNumRecords()) :
                -Integer.compare(x.getItems().size(), y.getItems().size()));

        return ret;
    }
}
