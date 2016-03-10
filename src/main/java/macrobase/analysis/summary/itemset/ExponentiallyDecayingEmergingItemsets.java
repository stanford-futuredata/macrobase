package macrobase.analysis.summary.itemset;

import static com.codahale.metrics.MetricRegistry.name;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import macrobase.MacroBase;
import macrobase.analysis.summary.count.ApproximateCount;
import macrobase.analysis.summary.count.FastButBigSpaceSaving;
import macrobase.analysis.summary.itemset.result.ItemsetResult;
import macrobase.analysis.summary.itemset.result.ItemsetWithCount;
import macrobase.datamodel.Datum;
import macrobase.ingest.DatumEncoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.google.common.collect.Sets;

public class ExponentiallyDecayingEmergingItemsets {
    private static final Logger log = LoggerFactory.getLogger(ExponentiallyDecayingEmergingItemsets.class);

    private static final Timer inlierDecayTime = MacroBase.metrics.timer(
            name(ExponentiallyDecayingEmergingItemsets.class, "inlierDecayTime"));
    private static final Timer outlierDecayTime = MacroBase.metrics.timer(
            name(ExponentiallyDecayingEmergingItemsets.class, "outlierDecayTime"));


    private double numInliers;
    private double numOutliers;

    private final double minSupportOutlier;
    private final double minRatio;
    private final double exponentialDecayRate;

    private final ApproximateCount outlierCountSummary;
    private final ApproximateCount inlierCountSummary;
    private final StreamingFPGrowth outlierPatternSummary;
    private final StreamingFPGrowth inlierPatternSummary = new StreamingFPGrowth(0);
    private final int attributeDimension;

    public ExponentiallyDecayingEmergingItemsets(int inlierSummarySize,
                                                 int outlierSummarySize,
                                                 double minSupportOutlier,
                                                 double minRatio,
                                                 double exponentialDecayRate,
                                                 int attributeDimension) {
        this.minSupportOutlier = minSupportOutlier;
        this.minRatio = minRatio;
        this.exponentialDecayRate = exponentialDecayRate;
        this.attributeDimension = attributeDimension;

        outlierCountSummary = new FastButBigSpaceSaving(outlierSummarySize);
        inlierCountSummary = new FastButBigSpaceSaving(inlierSummarySize);
        outlierPatternSummary = new StreamingFPGrowth(minSupportOutlier);
    }

    Map<Integer, Double> interestingItems;

    public Double getInlierCount() {
        return numInliers;
    }

    public Double getOutlierCount() {
        return numOutliers;
    }

    public void updateModelsNoDecay() {
        updateModels(false);
    }

    public void updateModelsAndDecay() {
        updateModels(true);
    }

    private void updateModels(boolean doDecay) {
        if (attributeDimension == 1) {
            return;
        }

        Map<Integer, Double> outlierCounts = this.outlierCountSummary.getCounts();
        Map<Integer, Double> inlierCounts = this.inlierCountSummary.getCounts();

        int supportCountRequired = (int) (this.outlierCountSummary.getTotalCount() * minSupportOutlier);

        interestingItems = new HashMap<>();

        for (Map.Entry<Integer, Double> outlierCount : outlierCounts.entrySet()) {
            if (outlierCount.getValue() < supportCountRequired) {
                continue;
            }

            Double inlierCount = inlierCounts.get(outlierCount.getKey());

            if (inlierCount != null &&
                ((outlierCount.getValue() / this.outlierCountSummary.getTotalCount() /
                  (inlierCount / this.inlierCountSummary.getTotalCount()) < minRatio))) {
                continue;
            }

            interestingItems.put(outlierCount.getKey(), outlierCount.getValue());
        }

        log.trace("found {} interesting items", interestingItems.size());

        Timer.Context ot = outlierDecayTime.time();
        outlierPatternSummary.decayAndResetFrequentItems(interestingItems, doDecay ? exponentialDecayRate : 0);
        ot.stop();

        Timer.Context it = inlierDecayTime.time();
        inlierPatternSummary.decayAndResetFrequentItems(interestingItems, doDecay ? exponentialDecayRate : 0);
        it.stop();
    }

    public void markPeriod() {
        outlierCountSummary.multiplyAllCounts(1 - exponentialDecayRate);
        inlierCountSummary.multiplyAllCounts(1 - exponentialDecayRate);

        updateModelsAndDecay();
    }

    public void markOutlier(Datum outlier) {
        numOutliers++;
        outlierCountSummary.observe(outlier.getAttributes());

        if (attributeDimension > 1) {
            outlierPatternSummary.insertTransactionStreamingFalseNegative(outlier.getAttributes());
        }
    }

    // TODO: don't track *all* inliers
    public void markInlier(Datum inlier) {
        numInliers++;
        inlierCountSummary.observe(inlier.getAttributes());
        if (attributeDimension > 1) {
            inlierPatternSummary.insertTransactionStreamingFalseNegative(inlier.getAttributes());
        }
    }

    private List<ItemsetResult> getSingleItemItemsets(DatumEncoder encoder) {
        double supportCountRequired = outlierCountSummary.getTotalCount() * minSupportOutlier;

        log.debug("REQUIRED SUPPORT: {} {}", supportCountRequired, minSupportOutlier);

        List<ItemsetResult> ret = new ArrayList<>();
        Map<Integer, Double> inlierCounts = inlierCountSummary.getCounts();
        Map<Integer, Double> outlierCounts = outlierCountSummary.getCounts();


        for (Map.Entry<Integer, Double> outlierCount : outlierCounts.entrySet()) {
            if (outlierCount.getValue() < supportCountRequired) {
                continue;
            }

            Double inlierCount = inlierCounts.get(outlierCount.getKey());

            double ratio;

            if (inlierCount != null) {
                ratio = (outlierCount.getValue() / this.outlierCountSummary.getTotalCount() /
                         (inlierCount / this.inlierCountSummary.getTotalCount()));
            } else {
                ratio = Double.POSITIVE_INFINITY;
            }

            if (ratio > minRatio) {
                ret.add(new ItemsetResult(outlierCount.getValue() / outlierCountSummary.getTotalCount(),
                                          outlierCount.getValue(),
                                          ratio,
                                          encoder.getColsFromAttr(outlierCount.getKey())));
            }
        }

        return ret;
    }

    public List<ItemsetResult> getItemsets(DatumEncoder encoder) {
        List<ItemsetResult> singleItemsets = getSingleItemItemsets(encoder);

        if (attributeDimension == 1) {
            return singleItemsets;
        }

        List<ItemsetWithCount> iwc = outlierPatternSummary.getItemsets();

        iwc.sort((x, y) -> x.getCount() != y.getCount() ?
                -Double.compare(x.getCount(), y.getCount()) :
                -Double.compare(x.getItems().size(), y.getItems().size()));

        Set<Integer> ratioItemsToCheck = new HashSet<>();
        List<ItemsetWithCount> ratioSetsToCheck = new ArrayList<>();
        List<ItemsetResult> ret = singleItemsets;

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

            if (i.getItems().size() != 1) {
                ratioItemsToCheck.addAll(i.getItems());
                ratioSetsToCheck.add(i);
            }
        }

        // check the ratios of any itemsets we just marked
        List<ItemsetWithCount> matchingInlierCounts = inlierPatternSummary.getCounts(ratioSetsToCheck);

        assert (matchingInlierCounts.size() == ratioSetsToCheck.size());
        for (int i = 0; i < matchingInlierCounts.size(); ++i) {
            ItemsetWithCount ic = matchingInlierCounts.get(i);
            ItemsetWithCount oc = ratioSetsToCheck.get(i);

            double ratio;
            if (ic.getCount() > 0) {
                ratio = (oc.getCount() / outlierCountSummary.getTotalCount()) / (ic.getCount() / inlierCountSummary.getTotalCount());
            } else {
                ratio = Double.POSITIVE_INFINITY;
            }

            if (ratio >= minRatio) {
                ret.add(new ItemsetResult(oc.getCount() / outlierCountSummary.getTotalCount(),
                                          oc.getCount(),
                                          ratio,
                                          encoder.getColsFromAttrSet(oc.getItems())));
            }
        }

        // finally sort one last time
        ret.sort((x, y) -> x.getNumRecords() != y.getNumRecords() ?
                -Double.compare(x.getNumRecords(), y.getNumRecords()) :
                -Double.compare(x.getItems().size(), y.getItems().size()));

        return ret;

    }
}
