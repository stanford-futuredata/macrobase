package edu.stanford.futuredata.macrobase.analysis.summary.fpg;

import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.result.FPGAttributeSet;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.result.FPGItemsetResult;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.result.ItemsetWithCount;
import edu.stanford.futuredata.macrobase.analysis.summary.util.AttributeEncoder;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.operator.IncrementalOperator;

import java.util.*;
import java.util.function.DoublePredicate;

/**
 * Given batches of rows with an outlier class column, explain the outliers using
 * string attribute columns. Results from each batch accumulates, until batches are retired.
 *
 * This class searches for new candidate explanations based on the latest pane. Viable candidates
 * are promoted and tracked until their support drops below a certain threshold, at which point they
 * are retired.
 */
public class IncrementalSummarizer implements IncrementalOperator<Explanation> {
    // Number of panes that we keep track in the summarizer
    private int numPanes;
    // Default parameters for the summarizer
    private String outlierColumn = "_OUTLIER";
    private double minOutlierSupport = 0.1;
    private double minRiskRatio = 3.0;
    private List<String> attributes = new ArrayList<>();
    // Default predicate for filtering outlying rows
    private DoublePredicate predicate = d -> d != 0.0;

    // Encoder and encoded attribute sets
    private AttributeEncoder encoder = new AttributeEncoder();
    private List<Set<Integer>> inlierItemsets, outlierItemsets;

    // Internal book keeping
    private Deque<HashMap<Set<Integer>, Double>> inlierItemsetPaneCounts;
    private Deque<HashMap<Set<Integer>, Double>> outlierItemsetPaneCounts;
    private HashMap<Set<Integer>, Double> inlierItemsetPaneCount = new HashMap<>();
    private HashMap<Set<Integer>, Double> outlierItemsetPaneCount = new HashMap<>();
    private HashMap<Set<Integer>, Double> inlierItemsetWindowCount = new HashMap<>();
    private HashMap<Set<Integer>, Double> outlierItemsetWindowCount = new HashMap<>();
    private Deque<Integer> inlierPaneCounts;
    private Deque<Integer> outlierPaneCounts;
    private HashMap<Set<Integer>, Integer> trackingMap = new HashMap<>();

    // Temp Intermediate Values
    private List<Integer> inlierCountCumSum;
    private List<Integer> outlierCountCumSum;

    public IncrementalSummarizer(int numPanes) {
        setWindowSize(numPanes);
        initializePanes();
    }

    public IncrementalSummarizer() {
        setWindowSize(1);
        initializePanes();
    }

    protected IncrementalSummarizer initializePanes() {
        inlierPaneCounts = new ArrayDeque<>(numPanes);
        outlierPaneCounts = new ArrayDeque<>(numPanes);
        inlierItemsetPaneCounts = new ArrayDeque<>(numPanes);
        outlierItemsetPaneCounts = new ArrayDeque<>(numPanes);
        return this;
    }

    @Override
    public void setWindowSize(int numPanes) {
        this.numPanes = numPanes;
    }
    @Override
    public int getWindowSize() { return numPanes; }

    public void setMinSupport(double minSupport) {
        this.minOutlierSupport = minSupport;
    }
    public double getMinSupport() { return minOutlierSupport; }
    public void setMinRiskRatio(double minRiskRatio) { this.minRiskRatio = minRiskRatio; }
    public double getMinRiskRatio() { return minRiskRatio; }

    /**
     * By default, will check for nonzero entries in a column of doubles.
     * @param predicate function to signify whether row should be treated as outlier.
     * @return this
     */
    public IncrementalSummarizer setOutlierPredicate(DoublePredicate predicate) {
        this.predicate = predicate;
        return this;
    }
    public DoublePredicate getOutlierPredicate() { return predicate; }

    public IncrementalSummarizer setAttributes(List<String> attributes) {
        this.attributes = attributes;
        this.encoder.setColumnNames(attributes);
        return this;
    }
    public List<String> getAttributes() { return attributes; }

    /**
     * Set the column which indicates outlier status. "_OUTLIER" by default.
     * @param outlierColumn new outlier indicator column.
     * @return this
     */
    public IncrementalSummarizer setOutlierColumn(String outlierColumn) {
        this.outlierColumn = outlierColumn;
        return this;
    }
    public String getOutlierColumn() { return outlierColumn; }

    /* Split dataframe into inliers and outliers and encode attributes into itemsets
     *
     * Variables afftected:
     *   - inlierItemsets:  Encoded inlier itemsets for this pane
     *   - outlierItemsets: Encoded outlier itemsets for this pane
     */
    private void encodeAttributes(DataFrame df) {
        // Filter inliers and outliers
        DataFrame outlierDF = df.filter(outlierColumn, predicate);
        DataFrame inlierDF = df.filter(outlierColumn, predicate.negate());

        // Encode inlier and outlier attribute columns
        if (attributes.isEmpty()) {
            encoder.setColumnNames(df.getSchema().getColumnNamesByType(Schema.ColType.STRING));
            inlierItemsets = encoder.encodeAttributesAsSets(inlierDF.getStringCols());
            outlierItemsets = encoder.encodeAttributesAsSets(outlierDF.getStringCols());
        } else {
            encoder.setColumnNames(attributes);
            inlierItemsets = encoder.encodeAttributesAsSets(inlierDF.getStringColsByName(attributes));
            outlierItemsets = encoder.encodeAttributesAsSets(outlierDF.getStringColsByName(attributes));
        }
    }

    /* Helper function to calculate cumulative pane count. This way, the support from pane a to
     * the pane b can easily be calculated by outlierCountCumSum.get(b) - outlierCountCumSum.get(a)
     *
     * Variables affected:
     *   - inlierCountCumSum:
     *   - outlierCountCumSum:
     */
    private void calcCumSum() {
        int[] inlierCounts = inlierPaneCounts.stream().mapToInt(i->i).toArray();
        int[] outlierCounts = outlierPaneCounts.stream().mapToInt(i->i).toArray();

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
     *   - Remove pane from the count buffer
     *   - Update the window counts
     *   - Update tracking map for itemsets
     *
     * Variables affected:
     *   - inlierPaneCounts, outlierPaneCounts: remove expired pane from queue
     *   - inlierItemsetWindowCount, outlierItemsetWindowCount:
     *          remove counts from the expired pane from the window count
     *   - trackingMap:
     *          adjust the pane number indicating when we first started tracking an itemset
     */
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
     * and update corresponding itemset counts for the entire window.
     *
     * Variables affected:
     *   - inlierItemsetPaneCount, outlierItemsetPaneCount
     *   - inlierItemsetWindowCount, outlierItemsetWindowCount
     *   - inlierPaneCounts, outlierPaneCounts
     */
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
        inlierPaneCounts.add(inlierItemsets.size());
        outlierPaneCounts.add(outlierItemsets.size());
    }

    /* This function checks whether all itemsets that we are currently tracking counts of
     * still have enough outlier support (from the pane it first got promoted till now).
     *
     * Variables affected:
     *   - trackingMap: remove unsupported itemset from tracking map
     *   - outlierItemsetPaneCount, inlierItemsetPaneCount, outlierItemsetWindowCount, inlierItemsetWindowCount:
     *          remove unsupported itemset counts
     * */
    private void pruneUnsupported() {
        // Prune unsupported itemsets in the current pane
        HashSet<Set<Integer>> unSupported = new HashSet<>();
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

    /* This function checks whether all itemsets that we are currently tracking counts of
    * still have enough risk ratio (from the pane it first got promoted till now).
    *
    * Variables affected:
    *   - trackingMap: remove lowRR itemset from tracking map
    *   - outlierItemsetPaneCount, inlierItemsetPaneCount, outlierItemsetWindowCount, inlierItemsetWindowCount:
    *          remove lowRR itemset counts
    * */
    private void pruneLowRR() {
        // Prune itemsets without high enough risk ratio
        int currPanes = inlierPaneCounts.size();
        HashSet<Set<Integer>> lowRR = new HashSet<>();
        for (Set<Integer> itemset : outlierItemsetWindowCount.keySet()) {
            int outlierSupport = outlierCountCumSum.get(currPanes) - outlierCountCumSum.get(trackingMap.get(itemset));
            int inlierSupport = inlierCountCumSum.get(currPanes) - inlierCountCumSum.get(trackingMap.get(itemset));
            double rr = RiskRatio.compute(inlierItemsetWindowCount.get(itemset),
                    outlierItemsetWindowCount.get(itemset),
                    inlierSupport,
                    outlierSupport);
            // Add to output if the itemset has sufficient risk ratio
            if (rr < minRiskRatio) {
                lowRR.add(itemset);
            }
        }
        outlierItemsetPaneCount.keySet().removeAll(lowRR);
        inlierItemsetPaneCount.keySet().removeAll(lowRR);
        outlierItemsetWindowCount.keySet().removeAll(lowRR);
        inlierItemsetWindowCount.keySet().removeAll(lowRR);
    }

    /* Helper function that updates internals to keep track of a promoted itemset. */
    private void trackItemset(Set<Integer> itemset, double exposedOutlierCount, double exposedInlierCount) {
        trackingMap.put(itemset, inlierPaneCounts.size() - 1);
        outlierItemsetWindowCount.put(itemset, exposedOutlierCount);
        outlierItemsetPaneCount.put(itemset, exposedOutlierCount);
        inlierItemsetWindowCount.put(itemset, exposedInlierCount);
        inlierItemsetPaneCount.put(itemset, exposedInlierCount);
    }

    /* This function picks up itemsets that have enough outlier support and risk ratio in the current pane
     * (a.k.a. itemsets that can be promoted) and starts tracking their occurrences in the window.
     *
     * Variables affected:
     *   - trackingMap: record that we start tracking new frequent itemsets in this pane
     *   - outlierItemsetPaneCount, inlierItemsetPaneCount, outlierItemsetWindowCount, inlierItemsetWindowCount:
     *      add new frequent itemset counts
     */
    private void addNewFrequent() {
        // Return when the outlier population is too small, otherwise all
        // outlying combos in the current pane might get tracked
        if (minOutlierSupport * outlierItemsets.size() < 1) { return; }
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
                newFrequent.add(iwc);
            }
        }
        // Get support in the inlier transactions
        List<ItemsetWithCount> newFrequentInlierCounts = fpGrowth.getCounts(
                inlierItemsets, inlierPaneSingletonCount, inlierPaneSingletonCount.keySet(), newFrequent);
        for (int i = 0; i < newFrequent.size(); i ++) {
            ItemsetWithCount iiwc = newFrequentInlierCounts.get(i);
            ItemsetWithCount oiwc = newFrequent.get(i);
            double exposedInlierCount = iiwc.getCount();
            double exposedOutlierCount = oiwc.getCount();
            double rr = RiskRatio.compute(exposedInlierCount, exposedOutlierCount, inlierItemsets.size(), outlierItemsets.size());
            if (rr >= minRiskRatio) {
                Set<Integer> itemset = iiwc.getItems();
                trackItemset(itemset, exposedOutlierCount, exposedInlierCount);
            }
        }
    }

    @Override
    public void process(DataFrame df) {
        // 1. Retire old pane counts if necessary
        if (inlierPaneCounts.size() == numPanes) { expireLastPane(); }
        // 2. Add support counts for the new pane
        encodeAttributes(df);
        addNewPane();
        // 3. Prune unsupported outlier itemsets
        calcCumSum();
        pruneUnsupported();
        pruneLowRR();
        // 4. Compute new frequent outlier itemsets
        addNewFrequent();
        // Add final pane counts to buffer
        inlierItemsetPaneCounts.add(inlierItemsetPaneCount);
        outlierItemsetPaneCounts.add(outlierItemsetPaneCount);
    }

    /**
     * @return explanation
     */
    public FPGExplanation getResults() {
        long startTime = System.currentTimeMillis();

        calcCumSum();
        pruneUnsupported();

        int currPanes = inlierPaneCounts.size();
        List<FPGAttributeSet> attributeSets = new ArrayList<>();
        for (Set<Integer> itemset : outlierItemsetWindowCount.keySet()) {
            int outlierSupport = outlierCountCumSum.get(currPanes) - outlierCountCumSum.get(trackingMap.get(itemset));
            int inlierSupport = inlierCountCumSum.get(currPanes) - inlierCountCumSum.get(trackingMap.get(itemset));
            double rr = RiskRatio.compute(inlierItemsetWindowCount.get(itemset),
                    outlierItemsetWindowCount.get(itemset),
                    inlierSupport,
                    outlierSupport);
            // Add to output if the itemset has sufficient risk ratio
            if (rr >= minRiskRatio) {
                FPGItemsetResult result = new FPGItemsetResult(
                        outlierItemsetWindowCount.get(itemset) / outlierSupport,
                        outlierItemsetWindowCount.get(itemset),
                        rr,
                        itemset);
                attributeSets.add(new FPGAttributeSet(result, encoder));
            }
        }
        long elapsed = System.currentTimeMillis() - startTime;
        FPGExplanation explanation = new FPGExplanation(attributeSets,
                (long) inlierCountCumSum.get(currPanes),
                (long) outlierCountCumSum.get(currPanes),
                elapsed);
        return explanation;
    }

}
