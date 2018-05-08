package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.summary.BatchSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.AggregationOp;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.QualityMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.util.AttributeEncoder;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Generic summarizer superclass that can be customized with
 * different quality metrics and input sources. Subclasses are responsible
 * for converting from user-provided columns to the internal linear aggregates.
 */
public abstract class APLSummarizer extends BatchSummarizer {
    Logger log = LoggerFactory.getLogger("APLSummarizer");
    AttributeEncoder encoder;
    APLExplanation explanation;
    public APrioriLinear aplKernel;
    public APrioriBasic aplBasicKernel;
    List<QualityMetric> qualityMetricList;
    List<Double> thresholds;
    double sampleRate = 1.0;
    /* When the input has been sampled, this is the ratio of the input outlier rate
       to the true outlier rate. */
    double inlierWeight = 1.0;
    double outlierSampleRate;
    boolean calcErrors = false;
    int fullNumOutliers;
    boolean verbose = true;
    boolean basic = false;
    boolean simpleEncoding = false;
    double injectFraction = 0.0;
    boolean smartStopping = true;

    protected long numEvents = 0;
    protected long numOutliers = 0;

    public double encodingTime;
    public int numEncodedCategories;
    public double explanationTime;
    public int numResults;

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
        process(input, true);
    }

    public void process(DataFrame input, boolean doEncoding) throws Exception {
        long startTime, elapsed;

//        startTime = System.currentTimeMillis();
//        if (sampleRate < 1.0) {
//            input = input.sample(sampleRate);
//        }
//        elapsed = System.currentTimeMillis() - startTime;
//        log.info("Sampled in: {} ms", elapsed);

        int[][] encoded;
        if (doEncoding) {
            encoder = new AttributeEncoder();
            encoder.setColumnNames(attributes);
            startTime = System.currentTimeMillis();
            encoded = getEncoded(input.getStringColsByName(attributes), input);
            elapsed = System.currentTimeMillis() - startTime;
            if (verbose) {
                log.info("Encoded in: {} ms", elapsed);
                log.info("Encoded Categories: {}", encoder.getNextKey() - 1);
            } else {
                encodingTime = elapsed;
                numEncodedCategories = encoder.getNextKey() - 1;
            }
        } else {
            startTime = System.currentTimeMillis();
            encoded = AttributeEncoder.encodeAttributesAsArray(input.getStringColsByName(attributes), encoder);
            encodingTime = System.currentTimeMillis() - startTime;
        }

        thresholds = getThresholds();
        qualityMetricList = getQualityMetricList();
        if (basic) {
            aplBasicKernel = new APrioriBasic(
                    qualityMetricList,
                    thresholds
            );
        } else {
            aplKernel = new APrioriLinear(
                    qualityMetricList,
                    thresholds
            );
            aplKernel.setVerbose(verbose);
        }

        double[][] aggregateColumns = getAggregateColumns(input);
        List<String> aggregateNames = getAggregateNames();
        AggregationOp[] aggregationOps = getAggregationOps();
        startTime = System.currentTimeMillis();
        List<APLExplanationResult> aplResults;
        if (basic) {
            aplBasicKernel.setInjectFraction(injectFraction);
            aplBasicKernel.setSmartStopping(smartStopping);
            aplResults = aplBasicKernel.explain(
                    encoded,
                    aggregateColumns
            );
        } else {
            aplResults = aplKernel.explain(encoded,
                    aggregateColumns,
                    aggregationOps,
                    encoder.getNextKey(),
                    Math.min(maxOrder, attributes.size()),
                    numThreads,
                    calcErrors
            );
        }
        elapsed = System.currentTimeMillis() - startTime;
        if (verbose) {
            log.info("Explained in: {} ms", elapsed);
            log.info("Number of results: {}", aplResults.size());
        } else {
            explanationTime = elapsed;
            numResults = aplResults.size();
        }
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

    public APLExplanation getResults() {
        return explanation;
    }

    public void setSampleRate(double sampleRate) { this.sampleRate = sampleRate; }
    public void setInlierWeight(double inlierWeight) { this.inlierWeight = inlierWeight; }
    public void setOutlierSampleRate(double outlierSampleRate) { this.outlierSampleRate = outlierSampleRate; }
    public void setCalcErrors(boolean calcErrors) { this.calcErrors = calcErrors; }
    public void setFullNumOutliers(int fullNumOutliers) { this.fullNumOutliers = fullNumOutliers; }
    public void setVerbose(boolean verbose) { this.verbose = verbose; }
    public void setBasic(boolean basic) { this.basic = basic; }
    public void setSimpleEncoding(boolean simpleEncoding) { this.simpleEncoding = simpleEncoding; }
    public void setEncoder(AttributeEncoder encoder) { this.encoder = encoder; }
    public void setInjectFraction(double injectFraction) { this.injectFraction = injectFraction; }
    public void setSmartStopping(boolean smartStopping) { this.smartStopping = smartStopping; }
}
