package edu.stanford.futuredata.macrobase.analysis.summary;

import edu.stanford.futuredata.macrobase.analysis.summary.count.ExactCount;
import edu.stanford.futuredata.macrobase.analysis.summary.itemset.AttributeEncoder;
import edu.stanford.futuredata.macrobase.analysis.summary.itemset.FPGrowth;
import edu.stanford.futuredata.macrobase.analysis.summary.itemset.RiskRatio;
import edu.stanford.futuredata.macrobase.analysis.summary.itemset.result.AttributeSet;
import edu.stanford.futuredata.macrobase.analysis.summary.itemset.result.ItemsetResult;
import edu.stanford.futuredata.macrobase.analysis.summary.itemset.result.ItemsetWithCount;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.operator.IncrementalOperator;

import java.util.*;
import java.util.function.DoublePredicate;

/**
 * Given batches of rows with an outlier class column, explain the outliers using
 * string attribute columns. Results from each batch accumulates, until batches are retired.
 */
public class IncrementalSummarizer implements IncrementalOperator<Explanation> {
    //
    public int numPanes;

    // Default parameters for the summarizer
    protected String outlierColumn = "_OUTLIER";
    protected double minOutlierSupport = 0.1;
    protected List<String> attributes = new ArrayList<>();
    // Default predicate for filtering outlying rows
    protected DoublePredicate predicate = d -> d != 0.0;

    // Encoder and encoded attribute sets
    protected AttributeEncoder encoder = new AttributeEncoder();
    protected List<Set<Integer>> inlierItemsets, outlierItemsets;

    // Internal booking keep
    private Deque<HashMap<Set<Integer>, Double>> inlierItemsetPaneCounts;
    private Deque<HashMap<Set<Integer>, Double>> outlierItemsetPaneCounts;
    private HashMap<Set<Integer>, Double> inlierItemsetPaneCount = new HashMap<>();
    private HashMap<Set<Integer>, Double> outlierItemsetPaneCount = new HashMap<>();
    private HashMap<Set<Integer>, Double> inlierItemsetWindowCount = new HashMap<>();
    private HashMap<Set<Integer>, Double> outlierItemsetWindowCount = new HashMap<>();
    private Deque<Integer> inlierPaneCounts;
    private Deque<Integer> outlierPaneCounts;
    private List<Integer> inlierCountCumSum;
    private List<Integer> outlierCountCumSum;
    private HashMap<Set<Integer>, Integer> trackingMap = new HashMap<>();

    public IncrementalSummarizer(int numPanes) { setWindowSize(numPanes); }

    @Override
    public void setWindowSize(int numPanes) {
        this.numPanes = numPanes;
        if (inlierItemsetPaneCounts == null) {
            inlierPaneCounts = new ArrayDeque<>(numPanes);
            outlierPaneCounts = new ArrayDeque<>(numPanes);
            inlierItemsetPaneCounts = new ArrayDeque<>(numPanes);
            outlierItemsetPaneCounts = new ArrayDeque<>(numPanes);
        }
    }

    @Override
    public int getWindowSize() { return numPanes; }

    public IncrementalSummarizer setMinSupport(double minSupport) {
        this.minOutlierSupport = minSupport;
        return this;
    }

    /**
     * By default, will check for nonzero entries in a column of doubles.
     * @param predicate function to signify whether row should be treated as outlier.
     * @return this
     */
    public IncrementalSummarizer setOutlierPredicate(DoublePredicate predicate) {
        this.predicate = predicate;
        return this;
    }
    public IncrementalSummarizer setAttributes(List<String> attributes) {
        this.attributes = attributes;
        this.encoder.setColumnNames(attributes);
        return this;
    }

    /**
     * Set the column which indicates outlier status. "_OUTLIER" by default.
     * @param outlierColumn new outlier indicator column.
     * @return this
     */
    public IncrementalSummarizer setOutlierColumn(String outlierColumn) {
        this.outlierColumn = outlierColumn;
        return this;
    }

    /* Encode inlier and outlier attributes into itemsets */
    private void encodeAttributes(DataFrame df) {
        // Filter inliers and outliers
        DataFrame outlierDF = df.filter(outlierColumn, predicate);
        DataFrame inlierDF = df.filter(outlierColumn, predicate.negate());

        // Encode inlier and outlier attribute columns
        if (attributes.isEmpty()) {
            encoder.setColumnNames(df.getSchema().getColumnNamesByType(Schema.ColType.STRING));
            inlierItemsets = encoder.encodeAttributes(inlierDF.getStringCols());
            outlierItemsets = encoder.encodeAttributes(outlierDF.getStringCols());
        } else {
            encoder.setColumnNames(attributes);
            inlierItemsets = encoder.encodeAttributes(inlierDF.getStringColsByName(attributes));
            outlierItemsets = encoder.encodeAttributes(outlierDF.getStringColsByName(attributes));
        }
        inlierPaneCounts.add(inlierItemsets.size());
        outlierPaneCounts.add(outlierItemsets.size());
    }

    /* Helper function to calculate cumulative pane count. This way, the support from pane a to
     * the pane b can easily be calculated by outlierCountCumSum.get(b) - outlierCountCumSum.get(a) */
    private void calcCumSum() {
        Object[] inlierCounts = inlierPaneCounts.toArray();
        Object[] outlierCounts = outlierPaneCounts.toArray();

        inlierCountCumSum = new ArrayList<>(inlierPaneCounts.size() + 1);
        outlierCountCumSum = new ArrayList<>(inlierPaneCounts.size() + 1);
        inlierCountCumSum.add(0);
        outlierCountCumSum.add(0);
        for (int i = 0; i < inlierPaneCounts.size(); i ++) {
            inlierCountCumSum.add(inlierCountCumSum.get(i) + (int)inlierCounts[i]);
            outlierCountCumSum.add(outlierCountCumSum.get(i) + (int)outlierCounts[i]);
        }
    }

    /* This function retires the oldest pane:
        - Remove pane from the count buffer
        - Update the window counts
        - Update tracking map for itemsets */
    private void expireLastPane() {
        // Remove old pane counts from buffer
        HashMap<Set<Integer>, Double> inlierCounts = inlierItemsetPaneCounts.pollFirst();
        HashMap<Set<Integer>, Double> outlierCounts = outlierItemsetPaneCounts.pollFirst();
        inlierPaneCounts.pollFirst();
        outlierPaneCounts.pollFirst();
        for (Set<Integer> itemset : outlierCounts.keySet()) {
            double i = inlierItemsetWindowCount.getOrDefault(itemset, 0.0) - inlierCounts.getOrDefault(itemset, 0.0);
            double o = outlierItemsetWindowCount.getOrDefault(itemset, 0.0) - outlierCounts.get(itemset);
            if (i > 0) {
                inlierItemsetWindowCount.put(itemset, i);
            } else if (inlierItemsetWindowCount.containsKey(itemset)) { // Remove 0-count itemsets
                inlierItemsetWindowCount.remove(itemset);
            }
            if (o > 0) {
                outlierItemsetWindowCount.put(itemset, o);
            } else if (outlierItemsetWindowCount.containsKey(itemset)) { // Remove 0-count itemsets
                outlierItemsetWindowCount.remove(itemset);
            }
        }
        // Decrement pane number indicating where we first start tracking counts for an itemset
        // (since we removed the first pane)
        for (Set<Integer> itemset : trackingMap.keySet()) {
            int paneNo = trackingMap.get(itemset);
            if (paneNo > 0) {
                trackingMap.put(itemset, paneNo - 1);
            }
        }
    }

    /* This function counts the occurrence of each supported itemset in the new pane,
     *   and update corresponding itemset counts for the entire window. */
    private void addNewPane() {
        inlierItemsetPaneCount = new HashMap<>();
        outlierItemsetPaneCount = new HashMap<>();

        // Compute support for frequent itemsets in the new pane
        for (Set<Integer> supportedItem : outlierItemsetWindowCount.keySet()) {
            for (Set<Integer> itemset : inlierItemsets) {
                if (itemset.containsAll(supportedItem)) {
                    double curVal = inlierItemsetPaneCount.getOrDefault(supportedItem, 0.0);
                    inlierItemsetPaneCount.put(supportedItem, curVal + 1);
                }
            }
            for (Set<Integer> itemset : outlierItemsets) {
                if (itemset.containsAll(supportedItem)) {
                    double curVal = outlierItemsetPaneCount.getOrDefault(supportedItem, 0.0);
                    outlierItemsetPaneCount.put(supportedItem, curVal + 1);
                }
            }
        }

        // Update support for the window
        for (Set<Integer> itemset : outlierItemsetWindowCount.keySet()) {
            double i = inlierItemsetWindowCount.getOrDefault(itemset, 0.0) + inlierItemsetPaneCount.getOrDefault(itemset, 0.0);
            double o = outlierItemsetWindowCount.get(itemset) + outlierItemsetPaneCount.getOrDefault(itemset, 0.0);
            inlierItemsetWindowCount.put(itemset, i);
            outlierItemsetWindowCount.put(itemset, o);
        }
    }

    /* This function checks whether all itemsets that we are currently tracking counts of
     *  still have enough outlier support (from the pane it first got promoted till now). */
    private void pruneUnsupported() {
        HashSet<Set<Integer>> unSupported = new HashSet<>();

        // Prune unsupported itemsets in the current pane
        calcCumSum();
        int currPanes = inlierPaneCounts.size();
        for (Set<Integer> itemset : outlierItemsetWindowCount.keySet()) {
            double supportSinceTracked = outlierCountCumSum.get(currPanes) -
                    outlierCountCumSum.get(trackingMap.get(itemset));
            if (outlierItemsetWindowCount.getOrDefault(itemset, 0.0) < minOutlierSupport * supportSinceTracked) {
                unSupported.add(itemset);
                trackingMap.remove(itemset);
            }
        }
        outlierItemsetPaneCount.keySet().removeAll(unSupported);
        inlierItemsetPaneCount.keySet().removeAll(unSupported);
        outlierItemsetWindowCount.keySet().removeAll(unSupported);
        inlierItemsetWindowCount.keySet().removeAll(unSupported);
    }

    /* Helper function that updates internals to keep track of a promoted itemset. */
    private void trackItemset(Set<Integer> itemset, double count) {
        trackingMap.put(itemset, inlierPaneCounts.size() - 1);
        outlierItemsetWindowCount.put(itemset, count);
        outlierItemsetPaneCount.put(itemset, count);
    }

    /* This function picks up itemsets that have enough outlier support in the current pane
     * (a.k.a. itemsets that can be promoted) and starts tracking their occurances in the window. */
    private void addNewFrequent() {
        double minSupport = Math.ceil(minOutlierSupport * outlierItemsets.size());
        HashMap<Integer, Double> inlierPaneSingletonCount = new ExactCount().count(inlierItemsets).getCounts();
        // Get new frequent itemsets in outliers
        FPGrowth fpGrowth = new FPGrowth();
        List<ItemsetWithCount> frequent = fpGrowth.getItemsetsWithSupportCount(outlierItemsets,
                minSupport);
        List<ItemsetWithCount> newFrequent = new ArrayList<>();
        for (ItemsetWithCount iwc : frequent) {
            Set<Integer> itemset = iwc.getItems();
            if (!outlierItemsetWindowCount.containsKey(itemset)) {
                trackItemset(itemset, iwc.getCount());
                newFrequent.add(iwc);
            }
        }
        // Get support in the inlier transactions
        List<ItemsetWithCount> inlierCount = fpGrowth.getCounts(
                inlierItemsets, inlierPaneSingletonCount, inlierPaneSingletonCount.keySet(), newFrequent);
        for (ItemsetWithCount iwc : inlierCount) {
            inlierItemsetWindowCount.put(iwc.getItems(), iwc.getCount());
            inlierItemsetPaneCount.put(iwc.getItems(), iwc.getCount());
        }
    }

    @Override
    public void process(DataFrame df) {
        // 1. Retire old pane counts if necessary
        if (inlierPaneCounts.size() == numPanes) { expireLastPane(); }
        // 2. Add support counts for the new pane
        encodeAttributes(df);
        addNewPane();
        if (outlierItemsets.size() * minOutlierSupport >= 1) {
            // 3. Prune unsupported outlier itemsets
            pruneUnsupported();
            // 4. Compute new frequent outlier itemsets
            addNewFrequent();
        }
        // Add final pane counts to buffer
        inlierItemsetPaneCounts.add(inlierItemsetPaneCount);
        outlierItemsetPaneCounts.add(outlierItemsetPaneCount);
    }

    /**
     * @param minRiskRatio lowest risk ratio to consider for meaningful explanations.
     *                     Adjust this to tune the severity (e.g. strength of correlation)
     *                     of the results returned.
     * @return explanation
     */
    public Explanation getResults(double minRiskRatio) {
        long startTime = System.currentTimeMillis();

        calcCumSum();
        int currPanes = outlierPaneCounts.size();
        List<AttributeSet> attributeSets = new ArrayList<>();
        for (Set<Integer> itemset : outlierItemsetWindowCount.keySet()) {
            int outlierSupport = outlierCountCumSum.get(currPanes) - outlierCountCumSum.get(trackingMap.get(itemset));
            int inlierSupport = inlierCountCumSum.get(currPanes) - inlierCountCumSum.get(trackingMap.get(itemset));
            double rr = RiskRatio.compute(inlierItemsetWindowCount.get(itemset),
                    outlierItemsetWindowCount.get(itemset),
                    inlierSupport,
                    outlierSupport);
            // Add to output if the itemset has sufficient risk ratio
            if (rr >= minRiskRatio) {
                ItemsetResult result = new ItemsetResult(
                        outlierItemsetWindowCount.get(itemset) / outlierSupport,
                        outlierItemsetWindowCount.get(itemset),
                        rr,
                        itemset);
                attributeSets.add(new AttributeSet(result, encoder));
            }
        }
        long elapsed = System.currentTimeMillis() - startTime;
        Explanation explanation = new Explanation(attributeSets,
                (long) inlierCountCumSum.get(currPanes),
                (long) outlierCountCumSum.get(currPanes),
                elapsed);
        return explanation;
    }

    /* Use a default risk ratio of 3 if the users don't specify the minimum required risk ratio. */
    @Override
    public Explanation getResults() {
        return getResults(3);
    }
}
