package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.summary.BatchSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.AggregationOp;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.QualityMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.util.AttributeEncoder;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Generic summarizer superclass that can be customized with
 * different quality metrics and input sources. Subclasses are responsible
 * for converting from user-provided columns to the internal linear aggregates.
 */
public abstract class APLSummarizer extends BatchSummarizer {
    Logger log = LoggerFactory.getLogger("APLSummarizer");
    AttributeEncoder encoder;
    APLExplanation explanation;
    APrioriLinear aplKernel;
    List<QualityMetric> qualityMetricList;
    List<Double> thresholds;

    protected long numEvents = 0;
    protected long numOutliers = 0;
    protected int bitmapRatioThreshold = 256;
    double alpha = 0.5;

    public abstract List<String> getAggregateNames();
    public abstract AggregationOp[] getAggregationOps();
    public abstract double[][] getAggregateColumns(DataFrame input);
    public abstract List<QualityMetric> getQualityMetricList();
    public abstract List<Double> getThresholds();
    public abstract int[][] getEncoded(List<String[]> columns, DataFrame input);
    public abstract double getNumberOutliers(double[][] aggregates);

    protected double[] processCountCol(DataFrame input, String countColumn, int numRows) {
        double[] countCol;
        if (countColumn != null) {
            countCol = input.getDoubleColumnByName(countColumn);
            for (int i = 0; i < numRows; i++) {
                numEvents += countCol[i];
            }
        } else {
            countCol = new double[numRows];
            for (int i = 0; i < numRows; i++) {
                countCol[i] = 1.0;
            }
            numEvents = numRows;
        }
        return countCol;
    }


    public void process(DataFrame input) throws Exception {
        encoder = new AttributeEncoder();
        encoder.setColumnNames(attributes);
        long startTime = System.currentTimeMillis();
        int[][] encoded = getEncoded(input.getStringColsByName(attributes), input);
        long elapsed = System.currentTimeMillis() - startTime;
        log.info("Encoded in: {} ms", elapsed);
        log.info("Encoded Categories: {}", encoder.getNextKey() - 1);

        thresholds = getThresholds();
        qualityMetricList = getQualityMetricList();
        aplKernel = new APrioriLinear(
                qualityMetricList,
                thresholds
        );

        double[][] aggregateColumns = getAggregateColumns(input);
        List<String> aggregateNames = getAggregateNames();
        AggregationOp[] aggregationOps = getAggregationOps();
        List<APLExplanationResult> aplResults = aplKernel.explain(encoded,
                aggregateColumns,
                aggregationOps,
                encoder.getNextKey(),
                maxOrder,
                numThreads,
                encoder.getBitmap(),
                encoder.getOutlierList(),
                encoder.getColCardinalities(),
                useFDs,
                functionalDependencies,
                bitmapRatioThreshold
        );
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

        double gErrB_ = 0.9;
        double bErrB_ = 0.6;
        double gVarB_ = 0.05;
        double bVarB_ = 0.1;
        if (aggregateNames.get(0).equals("XRayOutliers")) {
            Map<String, double[]> dataXRayMap = new HashMap<>();

            for (APLExplanationResult er : explanation.getResults()) {
                dataXRayMap.put(er.getStringedFeatureVector(encoder, attributes), er.getXRayTuple());
            }

            DataXRaySolver dataxray = new DataXRaySolver(alpha, gErrB_, bErrB_, gVarB_, bVarB_, dataXRayMap);
            dataxray.readData("DataXRayAppInsights.in"); // read data
            long XRayStartTime = System.currentTimeMillis();
            dataxray.solveFeatures(); // solve for features
            log.info("XRay Time: {}", System.currentTimeMillis() - XRayStartTime);
            dataxray.printBadFeature("DataXRayAppInsights.out");// save bad features
        }

    }

    public APLExplanation getResults() {
        return explanation;
    }

    public void setBitmapRatioThreshold(int bitmapRatioThreshold) {
        this.bitmapRatioThreshold = bitmapRatioThreshold;
    }

}
