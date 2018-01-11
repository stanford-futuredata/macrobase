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

    /**
     * Adjust this to tune the significance (e.g. number of rows affected) of the results returned.
     * @param minSupport lowest outlier support of the results returned.
     * @return this
     */
    public void setMinSupport(double minSupport) {
        this.minOutlierSupport = minSupport;
    }

    public void setAttributes(List<String> attributes) {
        this.attributes = attributes;
    }

    /**
     * Set the column which indicates outlier status. "_OUTLIER" by default.
     * @param outlierColumn new outlier indicator column.
     * @return this
     */
    public void setOutlierColumn(String outlierColumn) {
        this.outlierColumn = outlierColumn;
    }
    /**
     * Adjust this to tune the severity (e.g. strength of correlation) of the results returned.
     * @param minRatioMetric lowest risk ratio to consider for meaningful explanations.
     * @return this
     */

    public void setMinRatioMetric(double minRatioMetric) {
        this.minRatioMetric = minRatioMetric;
    }
}
