package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.util.MacrobaseException;

import java.util.function.DoublePredicate;

/**
 * RawThresholdClassifier classifies an outlier based on a predicate(e.g., equality, less than, greater than)
 * and a hard-coded sentinel value. Unlike {@link PercentileClassifier}, outlier values are not determined based on a
 * proportion of the values in the metric column. Instead, the outlier values are defined explicitly by the user in the
 * conf.yaml file; for example:
 * <code>
 *     classifier: "raw_threshold"
 *     metric: "usage"
 *     predicate: "=="
 *     value: 1.0
 * </code>
 * This would instantiate a RawThresholdClassifier that classifies every value in the "usage" column equal to 1.0
 * as an outlier. Currently, we support six different predicates: "==", "!=", "<", ">", "<=", and ">=".
 */
public class RawThresholdClassifier extends Classifier {

    private final DoublePredicate predicate;
    private DataFrame output;

    /**
     * @param metricName Column on which to classifier outliers
     * @param predicateStr Predicate used for classification: "==", "!=", "<", ">", "<=", or ">="
     * @param sentinel Sentinel value used when evaluating the predicate to determine outlier
     * @throws MacrobaseException
     */
    public RawThresholdClassifier(final String metricName, final String predicateStr, final double sentinel)
            throws MacrobaseException {
        super(metricName);
        this.predicate = getPredicate(predicateStr, sentinel);
    }

    /**
     * @return Lambda function corresponding to the ``predicateStr''. The Lambda function takes in a single
     * argument, which will correspond to the value in the metric column. (A closure is created around the ``sentinel''
     * parameter.)
     * @throws MacrobaseException
     */
    private DoublePredicate getPredicate(final String predicateStr, final double sentinel) throws MacrobaseException {
        switch (predicateStr) {
            case "==":
                return (double x) -> x == sentinel;
            case "!=":
                return (double x) -> x != sentinel;
            case "<":
                return (double x) -> x < sentinel;
            case ">":
                return (double x) -> x > sentinel;
            case "<=":
                return (double x) -> x <= sentinel;
            case ">=":
                return (double x) -> x >= sentinel;
            default:
                throw new MacrobaseException("RawThresholdClassifier: Predicate string " + predicateStr +
                        " not suppported.");
        }
    }

    /**
     * Scan through the metric column, and evaluate the predicate on every value in the column. The ``input'' DataFrame
     * remains unmodified; a copy is created and all modifications are made on the copy.
     * @throws Exception
     */
    @Override
    public void process(DataFrame input) throws Exception {
        double[] metrics = input.getDoubleColumnByName(columnName);
        int len = metrics.length;
        output = input.copy();
        double[] resultColumn = new double[len];
        for (int i = 0; i < len; i++) {
            final double curVal = metrics[i];
            if (predicate.test(curVal)) {
                resultColumn[i] = 1.0;
            }
        }
        output.addDoubleColumn(outputColumnName, resultColumn);
    }

    @Override
    public DataFrame getResults() {
        return output;
    }
}
