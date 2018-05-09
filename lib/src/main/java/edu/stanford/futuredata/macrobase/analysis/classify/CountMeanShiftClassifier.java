package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.analysis.classify.stats.MBPredicate;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.util.MacroBaseException;

import java.util.function.DoublePredicate;
import java.util.function.Predicate;

public class CountMeanShiftClassifier extends Classifier {
    private Predicate<String> strPredicate;
    private DoublePredicate doublePredicate;
    private DataFrame output;
    private String metricColumnName;
    private String meanColumnName;
    private boolean isStrPredicate;
    public static String outlierCountColumnName = "_OUTLIERCOUNT";
    public static String inlierCountColumnName = "_INLIERCOUNT";
    public static String outlierMeanSumColumnName = "_OUTLIERMEANSUM";
    public static String inlierMeanSumColumnName = "_INLIERMEANSUM";

    /**
     * @param metricColumnName Column on which to classify outliers
     * @param meanColumnName Column containing means whose shifts will be explained
     * @param predicateStr Predicate used for classification: "==" or "!="
     * @param sentinel String sentinel value used when evaluating the predicate to determine outlier
     * @throws MacroBaseException
     */
    public CountMeanShiftClassifier(
            final String metricColumnName,
            final String meanColumnName,
            final String predicateStr,
            final String sentinel
    ) throws MacroBaseException {
        super(meanColumnName);
        this.metricColumnName = metricColumnName;
        this.meanColumnName = meanColumnName;
        this.strPredicate = MBPredicate.getStrPredicate(predicateStr, sentinel);
        this.isStrPredicate = true;
    }

    /**
     * @param metricColumnName Column on which to classify outliers
     * @param meanColumnName Column containing means whose shifts will be explained
     * @param predicateStr Predicate used for classification: "==", "!=", "<", ">", "<=", or ">="
     * @param sentinel Double sentinel value used when evaluating the predicate to determine outlier
     */
    public CountMeanShiftClassifier(
            final String metricColumnName,
            final String meanColumnName,
            final String predicateStr,
            final double sentinel
    ) throws MacroBaseException {
        super(meanColumnName);
        this.metricColumnName = metricColumnName;
        this.meanColumnName = meanColumnName;
        this.doublePredicate = MBPredicate.getDoublePredicate(predicateStr, sentinel);
        this.isStrPredicate = false;
    }

    /**
     * Scan through the metric column, and evaluate the predicate on every value in the column. The ``input'' DataFrame
     * remains unmodified; a copy is created and all modifications are made on the copy.  Then store counts and
     * meancounts for both outliers and inliers.
     * @throws Exception
     */
    @Override
    public void process(DataFrame input) throws Exception {
        String[] stringMetrics = null;
        if (isStrPredicate)
            stringMetrics = input.getStringColumnByName(metricColumnName);
        double[] doubleMetrics = null;
        if (!isStrPredicate)
            doubleMetrics = input.getDoubleColumnByName(metricColumnName);
        output = input.copy();
        double[] totalMeanColumn = input.getDoubleColumnByName(meanColumnName);
        int len = totalMeanColumn.length;
        double[] outlierCountColumn = new double[len];
        double[] inlierCountColumn = new double[len];
        double[] outlierMeanColumn = new double[len];
        double[] inlierMeanColumn = new double[len];
        for (int i = 0; i < len; i++) {
            if ((isStrPredicate && strPredicate.test(stringMetrics[i])) ||
                    (!isStrPredicate && doublePredicate.test(doubleMetrics[i]))) {
                outlierCountColumn[i] = 1.0;
                outlierMeanColumn[i] = totalMeanColumn[i];
            } else {
                inlierCountColumn[i] = 1.0;
                inlierMeanColumn[i] = totalMeanColumn[i];
            }
        }
        output.addColumn(outlierCountColumnName, outlierCountColumn);
        output.addColumn(inlierCountColumnName, inlierCountColumn);
        output.addColumn(outlierMeanSumColumnName, outlierMeanColumn);
        output.addColumn(inlierMeanSumColumnName, inlierMeanColumn);
    }


    @Override
    public DataFrame getResults() {
        return output;
    }
}
