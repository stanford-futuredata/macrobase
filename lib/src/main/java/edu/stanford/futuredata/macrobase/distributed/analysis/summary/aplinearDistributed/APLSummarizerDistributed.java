package edu.stanford.futuredata.macrobase.distributed.analysis.summary.aplinearDistributed;

import edu.stanford.futuredata.macrobase.analysis.summary.BatchSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLExplanation;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLExplanationResult;
import edu.stanford.futuredata.macrobase.analysis.summary.util.AttributeEncoder;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.QualityMetric;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Generic summarizer superclass that can be customized with
 * different quality metrics and input sources. Subclasses are responsible
 * for converting from user-provided columns to the internal linear aggregates.
 */
public abstract class APLSummarizerDistributed extends BatchSummarizer {
    Logger log = LoggerFactory.getLogger("APLSummarizerDistributed");
    AttributeEncoder encoder;
    private APLExplanation explanation;
    private JavaSparkContext sparkContext;
    private int numPartitions = 1;

    protected long numEvents = 0;
    protected long numOutliers = 0;

    public abstract List<String> getAggregateNames();
    public abstract double[][] getAggregateColumns(DataFrame input);
    public abstract List<QualityMetric> getQualityMetricList();
    public abstract List<Double> getThresholds();
    public abstract int[][] getEncoded(List<String[]> columns, DataFrame input);
    public abstract double getNumberOutliers(double[][] aggregates);

    APLSummarizerDistributed(JavaSparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }

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
        log.info("Encoded in: {}", elapsed);
        log.info("Encoded Categories: {}", encoder.getNextKey() - 1);

        List<Double> thresholds = getThresholds();
        List<QualityMetric> qualityMetricList = getQualityMetricList();

        double[][] aggregateColumns = getAggregateColumns(input);
        List<String> aggregateNames = getAggregateNames();
        List<APLExplanationResult> aplResults = APrioriLinearDistributed.explain(encoded,
                aggregateColumns,
                encoder.getNextKey(),
                numPartitions,
                qualityMetricList,
                thresholds,
                sparkContext
        );
        log.info("Number of results: {}", aplResults.size());
        numOutliers = (long)getNumberOutliers(aggregateColumns);

        explanation = new APLExplanation(
                encoder,
                numEvents,
                numOutliers,
                aggregateNames,
                qualityMetricList,
                thresholds,
                aplResults
        );
    }

    public APLExplanation getResults() {
        return explanation;
    }

    public void setNumPartitions(int numPartitions) {this.numPartitions = numPartitions;}

}
