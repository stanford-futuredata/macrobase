package edu.stanford.futuredata.macrobase.analysis.summary;

import edu.stanford.futuredata.macrobase.analysis.summary.itemset.AttributeEncoder;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.operator.Operator;

import java.util.ArrayList;
import java.util.List;
import java.util.function.DoublePredicate;

/**
 * Created by egan on 6/26/17.
 */
public abstract class BatchSummarizer implements Operator<DataFrame, Explanation> {
    // Parameters
    protected String outlierColumn = "_OUTLIER";
    protected double minOutlierSupport = 0.1;
    protected double minRiskRatio = 3;
    protected List<String> attributes = new ArrayList<>();
    protected DoublePredicate predicate = d -> d != 0.0;
    // Encoder
    protected AttributeEncoder encoder = new AttributeEncoder();

    /**
     * Adjust this to tune the significance (e.g. number of rows affected) of the results returned.
     * @param minSupport lowest outlier support of the results returned.
     * @return this
     */
    public BatchSummarizer setMinSupport(double minSupport) {
        this.minOutlierSupport = minSupport;
        return this;
    }

    /**
     * Adjust this to tune the severity (e.g. strength of correlation) of the results returned.
     * @param minRiskRatio lowest risk ratio to consider for meaningful explanations.
     * @return this
     */
    public BatchSummarizer setMinRiskRatio(double minRiskRatio) {
        this.minRiskRatio = minRiskRatio;
        return this;
    }

    /**
     * By default, will check for nonzero entries in a column of doubles.
     * @param predicate function to signify whether row should be treated as outlier.
     * @return this
     */
    public BatchSummarizer setOutlierPredicate(DoublePredicate predicate) {
        this.predicate = predicate;
        return this;
    }

    public BatchSummarizer setAttributes(List<String> attributes) {
        this.attributes = attributes;
        this.encoder.setColumnNames(attributes);
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
}
