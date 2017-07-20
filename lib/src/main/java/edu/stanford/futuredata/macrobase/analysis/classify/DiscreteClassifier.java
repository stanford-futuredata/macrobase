package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

/**
 * Classify rows based on high / low values for a single column of aggregated data.
 * Returns a new dataframe with a column representation the classification status for
 * each row: 1.0 if outlier, 0.0 otherwise.
 */
public class DiscreteClassifier extends ThresholdClassifier {
    // Parameters
    private String countColumnName;
    private double percentile = 0.5;

    // Calculated values
    private DataFrame output;

    public DiscreteClassifier(String metricColumnName, String countColumnName) {
        super(metricColumnName);
        this.countColumnName = countColumnName;
    }

    @Override
    public void process(DataFrame input) {
        double[] aggregatedMetrics = input.getDoubleColumnByName(columnName);
        double[] counts = input.getDoubleColumnByName(countColumnName);
        int len = aggregatedMetrics.length;
        int numRawMetrics = 0;
        for (int i = 0; i < len; i++) {
            numRawMetrics += counts[i];
        }
        double[] rawMetrics = new double[numRawMetrics];
        int cumRawMetrics = 0;
        for (int i = 0; i < len; i++) {
            for (int j = cumRawMetrics; j < cumRawMetrics + counts[i]; j++) {
                rawMetrics[j] = aggregatedMetrics[i];
            }
            cumRawMetrics += counts[i];
        }
        lowCutoff = new Percentile().evaluate(rawMetrics, percentile);
        highCutoff = new Percentile().evaluate(rawMetrics, 100.0 - percentile);

        output = input.copy();
        double[] resultColumn = new double[len];
        for (int i = 0; i < len; i++) {
            double curVal = aggregatedMetrics[i];
            if ((curVal > highCutoff && includeHigh)
                    || (curVal < lowCutoff && includeLow)
                    ) {
                resultColumn[i] = 1.0;
            }
        }
        output.addDoubleColumn(outputColumnName, resultColumn);
    }

    @Override
    public DataFrame getResults() {
        return output;
    }

    // Parameter Getters and Setters
    public double getPercentile() {
        return percentile;
    }

    /**
     * @param percentile Cutoff point for high or low values
     * @return this
     */
    public DiscreteClassifier setPercentile(double percentile) {
        this.percentile = percentile;
        return this;
    }

    public String getCountColumnName() {
        return countColumnName;
    }

    /**
     * @param countColumnName Which column contains the count of each row's attribute
     *                        combination. Only applicable for cubed data. Will be null
     *                        for raw data.
     * @return this
     */
    public DiscreteClassifier setCountColumnName(String countColumnName) {
        this.countColumnName = countColumnName;
        return this;
    }
}
