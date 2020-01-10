package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.summary.util.AttributeEncoder;
import edu.stanford.futuredata.macrobase.analysis.summary.util.IntSet;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.AggregationOp;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AntiDiffSubtractSummarizer extends APLOutlierSummarizer {

    private Logger log = LoggerFactory.getLogger("AntiDiffSubtractSummarizer");

    public AntiDiffSubtractSummarizer(boolean useBitmaps) {
        super(useBitmaps);
    }

    public void process(DataFrame input) {

        encoder = new AttributeEncoder();
        encoder.setColumnNames(attributes);
        long startTime = System.currentTimeMillis();
        int[][] encoded = getEncoded(input.getStringColsByName(attributes), input);
        long elapsed = System.currentTimeMillis() - startTime;
        log.info("Encoded in: {} ms", elapsed);
        log.info("Distinct values encoded: {}", encoder.getNextKey() - 1);

        thresholds = getThresholds();
        qualityMetricList = getQualityMetricList();
        final AllCombosKernel allCombosKernel = new AllCombosKernel(
            qualityMetricList,
            thresholds,
            encoder
        );
        aplKernel = new APrioriLinear(
            qualityMetricList,
            thresholds,
            encoder
        );

        double[][] aggregateColumns = getAggregateColumns(input);
        List<String> aggregateNames = getAggregateNames();
        AggregationOp[] aggregationOps = getAggregationOps();

        startTime = System.currentTimeMillis();
        final List<APLExplanationResult> allCombos = allCombosKernel.explain(encoded,
            aggregateColumns,
            globalAggregateCols == null ?  aggregateColumns : globalAggregateCols,
            aggregationOps,
            encoder.getNextKey(),
            maxOrder,
            numThreads,
            encoder.getBitmap(),
            encoder.getOutlierList(),
            encoder.getColCardinalities(),
            bitmapRatioThreshold
        );
        elapsed = System.currentTimeMillis() - startTime;
        log.info("All combos time: {} ms", elapsed);


        List<APLExplanationResult> aplResultsBeforeSubtract = aplKernel.explain(encoded,
            aggregateColumns,
            globalAggregateCols == null ?  aggregateColumns : globalAggregateCols,
            aggregationOps,
            encoder.getNextKey(),
            maxOrder,
            numThreads,
            encoder.getBitmap(),
            encoder.getOutlierList(),
            encoder.getColCardinalities(),
            useFDs,
            functionalDependencies,
            bitmapRatioThreshold,
            false
        );

        Set<IntSet> attrsInDiff = aplResultsBeforeSubtract.stream().map(x -> x.matcher).collect(Collectors.toSet());
        startTime = System.currentTimeMillis();
        final List<APLExplanationResult> aplResults = subtract(allCombos, attrsInDiff);
        elapsed = System.currentTimeMillis() - startTime;
        log.info("Subtraction time: {} ms", elapsed);

        log.info("Number of results: {}", aplResults.size());
        numOutliers = (long)getNumberOutliers(aggregateColumns);

        explanation = new APLExplanation(
            encoder,
            numEvents,
            numOutliers,
            aggregateNames,
            qualityMetricList,
            aplResults
        );
    }

    private List<APLExplanationResult> subtract(List<APLExplanationResult> allResults, Set<IntSet> attrsInDef) {
        List<APLExplanationResult> notInDiff = new ArrayList<>();
        for (APLExplanationResult result : allResults) {
            // if the attributes---or any subset of them--do not appear in the DIFF output, then
            // we include them
            boolean add = true;
            for (IntSet combo : result.matcher.getCombos()) {
                if (attrsInDef.contains(combo)) {
                    add = false;
                    break;
                }
            }
            if (add) {

                notInDiff.add(result);
            }
        }
        return notInDiff;
    }

    public APLExplanation getResults() {
        return explanation;
    }
}
