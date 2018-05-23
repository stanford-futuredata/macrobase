package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.summary.BatchSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.util.IntSet;
import edu.stanford.futuredata.macrobase.analysis.summary.util.IntSetAsArray;
import edu.stanford.futuredata.macrobase.analysis.summary.util.IntSetAsLong;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.AggregationOp;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.InterventionQualityMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.QualityMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.util.AttributeEncoder;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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


        numOutliers = (long)getNumberOutliers(aggregateColumns);

        List<APLExplanationResult> aplResults = new ArrayList<>();

        final int numAggregates = aggregateColumns.length;
        final int numRows = aggregateColumns[0].length;
        final int numColumns = encoded[0].length;

        // Row store for more convenient access
        final double[][] aRows = new double[numRows][numAggregates];
        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < numAggregates; j++) {
                aRows[i][j] = aggregateColumns[j][i];
            }
        }

        Map<IntSet, Integer> cubeMap = new HashMap<>();
        for (int i = 0; i < numRows; i++) {
            if (aRows[i][0] > 0.0) {
                for (int j = 0; j < numColumns; j++) {
                    IntSet set = new IntSetAsArray(encoded[i][j]);
                    cubeMap.compute(set, (key, v) -> v == null ? 1 : v + 1);
                }
                for (int j = 0; j < numColumns; j++) {
                    for (int k = j + 1; k < numColumns; k++) {
                        IntSet set = new IntSetAsArray(encoded[i][j], encoded[i][k]);
                        cubeMap.compute(set, (key, v) -> v == null ? 1 : v + 1);
                    }
                }
                for (int j = 0; j < numColumns; j++) {
                    for (int k = j + 1; k < numColumns; k++) {
                        for (int l = k + 1; l < numColumns; l++) {
                            IntSet set = new IntSetAsArray(encoded[i][j], encoded[i][k], encoded[i][l]);
                            cubeMap.compute(set, (key, v) -> v == null ? 1 : v + 1);
                        }
                    }
                }
            }
        }

        for (IntSet curSet : cubeMap.keySet()) {
            double[] aggregates = new double[]{(double) cubeMap.get(curSet)};
            double[] metrics = new double[]{numOutliers - (double) cubeMap.get(curSet)};
            aplResults.add(
                    new APLExplanationResult(new QualityMetric[] {
                            new InterventionQualityMetric(0)}, curSet, aggregates, metrics)
            );
        }

        log.info("Number of results: {}", aplResults.size());

        explanation = new APLExplanation(
                encoder,
                numEvents,
                numOutliers,
                aggregateNames,
                qualityMetricList,
                aplResults
        );
    }

    public APLExplanation getResults() {
        return explanation;
    }

    public void setBitmapRatioThreshold(int bitmapRatioThreshold) {
        this.bitmapRatioThreshold = bitmapRatioThreshold;
    }

}
