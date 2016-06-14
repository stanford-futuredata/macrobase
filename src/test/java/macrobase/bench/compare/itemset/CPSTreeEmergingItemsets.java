package macrobase.bench.compare.itemset;

import com.codahale.metrics.Timer;
import com.google.common.collect.Sets;
import macrobase.MacroBase;
import macrobase.analysis.summary.count.AmortizedMaintenanceCounter;
import macrobase.analysis.summary.count.ApproximateCount;
import macrobase.analysis.summary.itemset.StreamingFPGrowth;
import macrobase.analysis.summary.itemset.result.ItemsetResult;
import macrobase.analysis.summary.itemset.result.ItemsetWithCount;
import macrobase.datamodel.Datum;
import macrobase.ingest.DatumEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.codahale.metrics.MetricRegistry.name;

public class CPSTreeEmergingItemsets {
    private static final Logger log = LoggerFactory.getLogger(CPSTreeEmergingItemsets.class);

    private static final Timer inlierDecayTime = MacroBase.metrics.timer(
            name(CPSTreeEmergingItemsets.class, "inlierDecayTime"));
    private static final Timer outlierDecayTime = MacroBase.metrics.timer(
            name(CPSTreeEmergingItemsets.class, "outlierDecayTime"));


    private double numInliers;
    private double numOutliers;

    private final double minSupportOutlier;
    private final double minRatio;
    private final double exponentialDecayRate;

    private final StreamingFPGrowth outlierPatternSummary;
    private final StreamingFPGrowth inlierPatternSummary = new StreamingFPGrowth(0);
    private final int attributeDimension;

    private final boolean combinationsEnabled;

    public CPSTreeEmergingItemsets(int inlierSummarySize,
                                   int outlierSummarySize,
                                   double minSupportOutlier,
                                   double minRatio,
                                   double exponentialDecayRate,
                                   int attributeDimension,
                                   boolean combinationsEnabled) {
        this.minSupportOutlier = minSupportOutlier;
        this.minRatio = minRatio;
        this.exponentialDecayRate = exponentialDecayRate;
        this.attributeDimension = attributeDimension;
        this.combinationsEnabled = combinationsEnabled;

        outlierPatternSummary = new StreamingFPGrowth(minSupportOutlier);
    }

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
        numInliers *= exponentialDecayRate;
        numOutliers *= exponentialDecayRate;
    }

    private void updateModels(boolean doDecay) {
        Timer.Context ot = outlierDecayTime.time();
        outlierPatternSummary.decayAndResetFrequentItems(null, doDecay ? exponentialDecayRate : 0);
        ot.stop();

        Timer.Context it = inlierDecayTime.time();
        inlierPatternSummary.decayAndResetFrequentItems(null, doDecay ? exponentialDecayRate : 0);
        it.stop();
    }

    public void markPeriod() {
        updateModelsAndDecay();
    }

    public void markOutlier(Datum outlier) {
        numOutliers++;

        if (!combinationsEnabled || attributeDimension > 1) {
            outlierPatternSummary.insertTransactionStreamingExact(outlier.attributes());
        }
    }

    // TODO: don't track *all* inliers
    public void markInlier(Datum inlier) {
        numInliers++;
        if (!combinationsEnabled || attributeDimension > 1) {
            inlierPatternSummary.insertTransactionStreamingExact(inlier.attributes());
        }
    }

    public List<ItemsetResult> getItemsets(DatumEncoder encoder) {

        List<ItemsetWithCount> iwc = outlierPatternSummary.getItemsets();

        iwc.sort((x, y) -> x.getCount() != y.getCount() ?
                -Double.compare(x.getCount(), y.getCount()) :
                -Double.compare(x.getItems().size(), y.getItems().size()));

        Set<Integer> ratioItemsToCheck = new HashSet<>();
        List<ItemsetWithCount> ratioSetsToCheck = new ArrayList<>();

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
        List<ItemsetResult> ret = new ArrayList<>();

        assert (matchingInlierCounts.size() == ratioSetsToCheck.size());
        for (int i = 0; i < matchingInlierCounts.size(); ++i) {
            ItemsetWithCount ic = matchingInlierCounts.get(i);
            ItemsetWithCount oc = ratioSetsToCheck.get(i);

            double ratio;
            if (ic.getCount() > 0) {
                ratio = (oc.getCount() / numOutliers) / (ic.getCount() / numInliers);
            } else {
                ratio = Double.POSITIVE_INFINITY;
            }

            if (ratio >= minRatio) {
                ret.add(new ItemsetResult(oc.getCount() / numOutliers,
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
