package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.metrics.QualityMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.util.AttributeEncoder;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.operator.Operator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Generic summarizer superclass that can be customized with
 * different quality metrics and input sources. Subclasses are responsible
 * for converting from user-provided columns to the internal linear aggregates.
 */
public abstract class APLSummarizer implements Operator<DataFrame, APLExplanation> {
    Logger log = LoggerFactory.getLogger("APLSummarizer");
    List<String> attributes = new ArrayList<>();
    AttributeEncoder encoder;
    APLExplanation explanation;
    APrioriLinear aplKernel;

    protected long numEvents = 0;

    public abstract List<String> getAggregateNames();
    public abstract double[][] getAggregateColumns(DataFrame input);
    public abstract List<QualityMetric> getQualityMetricList();
    public abstract List<Double> getThresholds();

    protected double[] processCountCol(DataFrame input, String countColumn, int numRows) {
        double[] countCol = null;
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
        List<int[]> encoded = encoder.encodeAttributes(
                input.getStringColsByName(attributes)
        );
        long elapsed = System.currentTimeMillis() - startTime;
        log.debug("Encoded in: {}", elapsed);
        log.debug("Encoded Categories: {}", encoder.getNextKey());

        List<Double> thresholds = getThresholds();
        List<QualityMetric> qualityMetricList = getQualityMetricList();
        aplKernel = new APrioriLinear(
                qualityMetricList,
                thresholds
        );

        double[][] aggregateColumns = getAggregateColumns(input);
        List<String> aggregateNames = getAggregateNames();
        List<APLExplanationResult> aplResults = aplKernel.explain(encoded, aggregateColumns);

        explanation = new APLExplanation(
                encoder,
                numEvents,
                aggregateNames,
                qualityMetricList,
                thresholds,
                aplResults
        );
    }

    public APLExplanation getResults() {
        return explanation;
    }

    public void setAttributes(List<String> attributes) {
        this.attributes = attributes;
    }
}
