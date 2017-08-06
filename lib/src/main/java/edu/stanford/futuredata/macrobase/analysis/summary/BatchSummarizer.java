package edu.stanford.futuredata.macrobase.analysis.summary;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.operator.Operator;

import java.util.ArrayList;
import java.util.List;

/**
 * Takes a dataframe with binary classification and searches for explanations
 * (subgroup discovery / contrast set mining / feature selection)
 * that capture differences between the two groups.
 *
 * outlierColumn should either be 0.0 or 1.0 to signify outlying points or
 * a count of the number of outliers represented by a row
 */
public abstract class BatchSummarizer implements Operator<DataFrame, Explanation> {
    // Parameters
    protected String outlierColumn = "_OUTLIER";
    protected double minOutlierSupport = 0.1;
    protected double minRatioMetric = 3;
    protected List<String> attributes = new ArrayList<>();
    protected int maxOrder;

    /**
     * Adjust this to tune the significance (e.g. number of rows affected) of the results returned.
     * @param minSupport lowest outlier support of the results returned.
     * @return this
     */
    public BatchSummarizer setMinSupport(double minSupport) {
        this.minOutlierSupport = minSupport;
        return this;
    }

    public BatchSummarizer setAttributes(List<String> attributes) {
        this.attributes = attributes;
        return this;
    }

    /**
     * Set the column which indicates outlier status. "_OUTLIER" by default.
     * @param outlierColumn new outlier indicator column.
     * @return this
     */
    public BatchSummarizer setOutlierColumn(String outlierColumn) {
        this.outlierColumn = outlierColumn;
        return this;
    }

    /**
     * Adjust this to tune the severity (e.g. strength of correlation) of the results returned.
     * @param minRatio lowest risk ratio to consider for meaningful explanations.
     * @return this
     */
    public BatchSummarizer setMinRatioMetric(double minRatio) {
        this.minRatioMetric = minRatio;
        return this;
    }

    /**
     * Set the maximum order for explanations; i.e., maxOrder = 3, will find up
     * to 3-order explanations
     * @return this
     */
    public BatchSummarizer setMaxOrder(final int maxOrder) {
        this.maxOrder = maxOrder;
        return this;
    }
}
