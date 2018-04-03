package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.analysis.classify.stats.MBPredicate;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.util.MacroBaseException;

import java.util.function.DoublePredicate;
import java.util.function.Predicate;

/**
 * PredicateClassifier classifies outliers based on a predicate(e.g., equality, less than, greater than)
 * and a hard-coded sentinel value.
 *
 * In a cube, every group which matches the predicate will have all of its entries counted as outliers,
 * or otherwise none of its entries counted as outliers.
 */
public class PredicateCubeClassifier extends CubeClassifier {
    private DoublePredicate predicate;
    private Predicate<String> strPredicate;
    private DataFrame output;
    private String metricColumnName; //hack
    private boolean isStrPredicate;


    /**
     * @param metricColumnName Column on which to classifier outliers
     * @param predicateStr Predicate used for classification: "==", "!=", "<", ">", "<=", or ">="
     * @param sentinel Sentinel value used when evaluating the predicate to determine outlier
     * @throws MacroBaseException
     */
    public PredicateCubeClassifier(
            final String countColumnName,
            final String metricColumnName,
            final String predicateStr,
            final double sentinel
    ) throws MacroBaseException {
        super(countColumnName);
        this.metricColumnName = metricColumnName;
        this.predicate = MBPredicate.getDoublePredicate(predicateStr, sentinel);
        this.isStrPredicate = false;
    }


    /**
     * @param metricColumnName Column on which to classifier outliers
     * @param predicateStr Predicate used for classification: "==", "!=", "<", ">", "<=", or ">="
     * @param sentinel Sentinel value used when evaluating the predicate to determine outlier
     * @throws MacroBaseException
     */
    public PredicateCubeClassifier(
            final String countColumnName,
            final String metricColumnName,
            final String predicateStr,
            final String sentinel
    ) throws MacroBaseException {
        super(countColumnName);
        this.metricColumnName = metricColumnName;
        this.strPredicate = MBPredicate.getStrPredicate(predicateStr, sentinel);
        this.isStrPredicate = true;
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
        } else {
            processDouble(input);
        }
    }


    public void processDouble(DataFrame input) throws Exception {
        double[] metrics = input.getDoubleColumnByName(metricColumnName);
        int len = metrics.length;
        output = input.copy();
        double[] totalCountColumn = input.getDoubleColumnByName(getCountColumnName());
        double[] resultColumn = new double[len];
        for (int i = 0; i < len; i++) {
            final double curVal = metrics[i];
            if (predicate.test(curVal)) {
                resultColumn[i] = totalCountColumn[i];
            } else {
                resultColumn[i] = 0.0;
            }
        }
        output.addColumn(outputColumnName, resultColumn);
    }


    public void processString(DataFrame input) throws Exception {
        String[] metrics = input.getStringColumnByName(metricColumnName);
        int len = metrics.length;
        output = input.copy();
        double[] totalCountColumn = input.getDoubleColumnByName(getCountColumnName());
        double[] resultColumn = new double[len];
        for (int i = 0; i < len; i++) {
            final String curVal = metrics[i];
            if (strPredicate.test(curVal)) {
                resultColumn[i] = totalCountColumn[i];
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
