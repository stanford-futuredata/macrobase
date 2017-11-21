package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.analysis.classify.stats.LinearInterpolator;
import edu.stanford.futuredata.macrobase.analysis.classify.stats.WeightedPercentile;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;

import java.util.*;

/**
 * Classify rows by high / low values based on provided quantiles of the group.
 * Returns a new dataframe with a column representation of the estimated number of outliers
 * for each group, which can be non-integer.
 */
public class QuantileClassifier extends CubeClassifier implements ThresholdClassifier {
    // Parameters
    private List<String> quantileColumnNames;
    private double[] quantiles;
    private double percentile = 1.0;
    private boolean includeHigh = true;
    private boolean includeLow = true;

    // Calculated values
    private double lowCutoff;
    private double highCutoff;
    private DataFrame output;

    public QuantileClassifier(
            String countColumnName,
            LinkedHashMap<String, Double> quantileColumns
    ) {
        super(countColumnName);
        this.quantileColumnNames = new ArrayList<String>();
        this.quantiles = new double[quantileColumns.size()];
        int i = 0;
        for (Map.Entry<String, Double> entry : quantileColumns.entrySet()) {
            this.quantileColumnNames.add(entry.getKey());
            this.quantiles[i++] = entry.getValue();
        }
    }

    @Override
    public void process(DataFrame input) {
        double[] counts = input.getDoubleColumnByName(countColumnName);
        List<double[]> quantileColumns = input.getDoubleColsByName(quantileColumnNames);
        int len = counts.length;
        int numQuantiles = quantiles.length;

        double[] modifiedCounts = new double[len * (numQuantiles-1)];
        double[] modifiedMeans = new double[len * (numQuantiles-1)];
        for (int i = 0; i < len; i++) {
            for (int j = 0; j < quantiles.length - 1; j++) {
                modifiedCounts[i * (numQuantiles-1) + j] = (quantiles[j+1] - quantiles[j]) * counts[i];
                modifiedMeans[i * (numQuantiles-1) + j] =
                        (quantileColumns.get(j)[i] + quantileColumns.get(j+1)[i]) / 2.0;
            }
        }
        WeightedPercentile wp = new WeightedPercentile(modifiedCounts, modifiedMeans);
        lowCutoff = wp.evaluate(percentile);
        highCutoff = wp.evaluate(100.0 - percentile);

        output = input.copy();
        double[] resultColumn = new double[len];
        for (int i = 0; i < len; i++) {
            double count = counts[i];
            double[] quantileValues = new double[numQuantiles];
            for (int j = 0; j < numQuantiles; j++) {
                quantileValues[j] = quantileColumns.get(j)[i];
            }
            LinearInterpolator interpolator = new LinearInterpolator(quantileValues, quantiles);
            double numOutliers = 0.0;
            if (includeHigh) {
                if (highCutoff < quantileValues[0]) {
                    numOutliers += count;
                } else if (highCutoff < quantileValues[numQuantiles-1]) {
                    double highCutoffQuantile = interpolator.evaluate(highCutoff);
                    numOutliers += (1.0 - highCutoffQuantile) * count;
                }
            }
            if (includeLow) {
                if (lowCutoff > quantileValues[numQuantiles-1]) {
                    numOutliers += count;
                } else if (lowCutoff > quantileValues[0]) {
                    double lowCutoffQuantile = interpolator.evaluate(lowCutoff);
                    numOutliers += lowCutoffQuantile * count;
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
    public QuantileClassifier setPercentile(double percentile) {
        this.percentile = percentile;
        return this;
    }

    public List<String> getQuantileColumnNames() {
        return quantileColumnNames;
    }

    /**
     * @param quantileColumnNames Which columns contain the quantiles
     * @return this
     */
    public QuantileClassifier setQuantileColumnNames(List<String> quantileColumnNames) {
        this.quantileColumnNames = quantileColumnNames;
        return this;
    }

    public double[] getQuantiles() {
        return quantiles;
    }

    /**
     * @param quantiles The quantiles that are contained in the columns specified in
     *                  quantileColumnNames. The values should be between 0 and 1,
     *                  in increasing order, and ideally contain 0 and 1 (the min
     *                  and the max).
     * @return this
     */
    public QuantileClassifier setQuantiles(double[] quantiles) {
        this.quantiles = quantiles;
        return this;
    }

    public boolean isIncludeHigh() {
        return includeHigh;
    }

    /**
     * @param includeHigh Whether to count high points as outliers.
     * @return this
     */
    public QuantileClassifier setIncludeHigh(boolean includeHigh) {
        this.includeHigh = includeHigh;
        return this;
    }
    public boolean isIncludeLow() {
        return includeLow;
    }

    /**
     * @param includeLow Whether to count low points as outliers
     * @return this
     */
    public QuantileClassifier setIncludeLow(boolean includeLow) {
        this.includeLow = includeLow;
        return this;
    }

    public double getLowCutoff() {
        return lowCutoff;
    }
    public double getHighCutoff() {
        return highCutoff;
    }
}
