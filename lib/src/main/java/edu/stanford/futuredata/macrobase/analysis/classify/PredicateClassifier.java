package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.analysis.classify.stats.MBPredicate;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.util.MacrobaseException;

import java.util.List;
import java.util.function.DoublePredicate;
import java.util.function.Predicate;

/**
 * PredicateClassifier classifies an outlier based on a predicate(e.g., equality, less than, greater than)
 * and a hard-coded sentinel value. Unlike {@link PercentileClassifier}, outlier values are not determined based on a
 * proportion of the values in the metric column. Instead, the outlier values are defined explicitly by the user in the
 * conf.yaml file; for example:
 * <code>
 *     classifier: "raw_threshold"
 *     metric: "usage"
 *     predicate: "=="
 *     value: 1.0
 * </code>
 * This would instantiate a PredicateClassifier that classifies every value in the "usage" column equal to 1.0
 * as an outlier. Currently, we support six different predicates: "==", "!=", "<", ">", "<=", and ">=".
 */
public class PredicateClassifier extends Classifier {

    private DoublePredicate predicate;
    private Predicate<String> strPredicate;
    private DataFrame output;
    private boolean isStrPredicate;


    /**
     * @param columnName Column on which to classifier outliers
     * @param predicateStr Predicate used for classification: "==", "!=", "<", ">", "<=", or ">="
     * @param sentinel Sentinel value used when evaluating the predicate to determine outlier
     * @throws MacrobaseException
     */
    public PredicateClassifier(final String columnName, final String predicateStr, final double sentinel)
            throws MacrobaseException {
        super(columnName);
        this.predicate = MBPredicate.getDoublePredicate(predicateStr, sentinel);
        this.isStrPredicate = false;
    }


    /**
     * @param columnName Column on which to classifier outliers
     * @param predicateStr Predicate used for classification: "==", "!=", "<", ">", "<=", or ">="
     * @param sentinel Sentinel value used when evaluating the predicate to determine outlier
     * @throws MacrobaseException
     */
    public PredicateClassifier(final String columnName, final String predicateStr, final String sentinel)
            throws MacrobaseException {
        super(columnName);
        this.strPredicate = MBPredicate.getStrPredicate(predicateStr, sentinel);
        this.isStrPredicate = true;
    }

    /**
     * Alternate constructor that takes in List of Strings; used to instantiate Classifier (via
     * reflection) specified in MacroBase SQL query
     *
     * @param attrs by convention, should be a List that has 3 values: [outlier_col_name,
     * predicate type ("==","!=", etc.), sentinel (either String or double)]
     */
    public PredicateClassifier(final List<String> attrs) throws MacrobaseException {
        super(attrs.get(0));
        final String predicateStr = attrs.get(1);
        final String sentinelStr = attrs.get(2);
        try {
            final double sentinel = Double.parseDouble(sentinelStr);
            this.predicate = MBPredicate.getDoublePredicate(predicateStr, sentinel);
            this.isStrPredicate = false;
        } catch (NumberFormatException e) {
            this.strPredicate = MBPredicate.getStrPredicate(predicateStr, sentinelStr);
            this.isStrPredicate = true;
        } catch (MacrobaseException e) {
            throw e;
        }
    }



    /**
     * Scan through the metric column, and evaluate the predicate on every value in the column. The ``input'' DataFrame
     * remains unmodified; a copy is created and all modifications are made on the copy.
     * @throws Exception
     */
    @Override
    public void process(DataFrame input) throws Exception {
        if (isStrPredicate) {
            processString(input);
        }
        else {
            processDouble(input);
        }
    }


    public void processDouble(DataFrame input) throws Exception {
        double[] metrics = input.getDoubleColumnByName(columnName);
        int len = metrics.length;
        output = input.copy();
        double[] resultColumn = new double[len];
        for (int i = 0; i < len; i++) {
            final double curVal = metrics[i];
            if (predicate.test(curVal)) {
                resultColumn[i] = 1.0;
            } else {
                resultColumn[i] = 0.0;
            }
        }
        output.addColumn(outputColumnName, resultColumn);
    }


    public void processString(DataFrame input) throws Exception {
        String[] metrics = input.getStringColumnByName(columnName);
        int len = metrics.length;
        output = input.copy();
        double[] resultColumn = new double[len];
        for (int i = 0; i < len; i++) {
            final String curVal = metrics[i];
            if (strPredicate.test(curVal)) {
                resultColumn[i] = 1.0;
            } else {
                resultColumn[i] = 0.0;
            }
        }
        output.addColumn(outputColumnName, resultColumn);
    }


    @Override
    public DataFrame getResults() {
        return output;
    }
}
