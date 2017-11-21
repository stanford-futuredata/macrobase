package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.operator.Transformer;

/**
 * Classifier for cubed data where rows represent groups of attributes and some corresponding
 * aggregate metrics. The count column contains the number of raw events represented
 * in each row/group.
 * Returns a new dataframe with a column representation of the estimated number of
 * outliers in each group.
 */
public abstract class CubeClassifier implements Transformer {
    protected String countColumnName = "count";
    protected String outputColumnName = "_OUTLIER";

    public CubeClassifier(String countColumnName) {
        this.countColumnName = countColumnName;
    }

    public String getOutputColumnName() {
        return outputColumnName;
    }

    /**
     * @param outputColumnName Which column to write the classification results.
     * @return this
     */
    public CubeClassifier setOutputColumnName(String outputColumnName) {
        this.outputColumnName = outputColumnName;
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
    public CubeClassifier setCountColumnName(String countColumnName) {
        this.countColumnName = countColumnName;
        return this;
    }
}
