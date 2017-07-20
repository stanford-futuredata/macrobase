package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;

import java.lang.Double;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.commons.math3.distribution.NormalDistribution;

/**
 * Classify rows based on high / low values for a single column of aggregated data.
 * Returns a new dataframe with a column representation the classification status for
 * each row: 1.0 if outlier, 0.0 otherwise.
 */
public class ArithmeticClassifier extends ThresholdClassifier {
    // Parameters
    private String countColumnName = "count";
    private String stdColumnName = "std";
    private double percentile = 1.0;

    // Calculated values
    private DataFrame output;

    public ArithmeticClassifier(String countColumnName, String meanColumnName,
                                String stdColumnName) {
        super(meanColumnName);
        this.countColumnName = countColumnName;
        this.stdColumnName = stdColumnName;
    }

    @Override
    public void process(DataFrame input) {
        double[] aggregatedMetrics = input.getDoubleColumnByName(columnName);
        double[] counts = input.getDoubleColumnByName(countColumnName);
        double[] stdColumn = input.getDoubleColumnByName(stdColumnName);
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
            double std = stdColumn[i];
            double count = counts[i];
            double numOutliers = 0.0;
            if (Double.isNaN(std) || std == 0.0) {
                // only one metric in group, or all metrics are equal
                if ((includeHigh && curVal > highCutoff)
                        || (includeLow && curVal < highCutoff)) {
                    numOutliers = count;
                }
            } else {
                NormalDistribution dist = new NormalDistribution(curVal, std);
                if (includeHigh) {
                    double percentile = dist.cumulativeProbability(highCutoff);
                    numOutliers += count * (1.0 - percentile);
                }
                if (includeLow) {
                    double percentile = dist.cumulativeProbability(lowCutoff);
                    numOutliers += count * percentile;
                }
            }
            resultColumn[i] = numOutliers;
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
    public ArithmeticClassifier setPercentile(double percentile) {
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
    public ArithmeticClassifier setCountColumnName(String countColumnName) {
        this.countColumnName = countColumnName;
        return this;
    }

    public String getStdColumnName() {
        return stdColumnName;
    }

    /**
     * @param stdColumnName Which column contains the standard deviation of metrics for events
     *                      corresponding to each row's attribute combination.
     * @return this
     */
    public ArithmeticClassifier setStdColumnName(String stdColumnName) {
        this.stdColumnName = stdColumnName;
        return this;
    }
}
