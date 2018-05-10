package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.analysis.classify.stats.MBPredicate;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.util.MacroBaseException;

import java.util.function.DoublePredicate;
import java.util.function.Predicate;

public class CountMeanShiftCubedClassifier extends CubeClassifier {
    private Predicate<String> strPredicate;
    private DoublePredicate doublePredicate;
    private DataFrame output;
    private String metricColumnName;
    private String meanColumnName;
    private boolean isStrPredicate;
    public static String outlierCountColumnName = CountMeanShiftClassifier.outlierCountColumnName;
    public static String inlierCountColumnName = CountMeanShiftClassifier.inlierCountColumnName;
    public static String outlierMeanSumColumnName = CountMeanShiftClassifier.outlierMeanSumColumnName;
    public static String inlierMeanSumColumnName = CountMeanShiftClassifier.inlierMeanSumColumnName;

    /**
     * @param countColumnName Column containing per-row counts
     * @param metricColumnName Column on which to classify outliers
     * @param meanColumnName Column containing means whose shifts will be explained
     * @param predicateStr Predicate used for classification: "==" or "!="
     * @param sentinel String sentinel value used when evaluating the predicate to determine outlier
     * @throws MacroBaseException
     */
    public CountMeanShiftCubedClassifier(
            final String countColumnName,
            final String metricColumnName,
            final String meanColumnName,
            final String predicateStr,
            final String sentinel
    ) throws MacroBaseException {
        super(countColumnName);
        this.metricColumnName = metricColumnName;
        this.meanColumnName = meanColumnName;
        this.strPredicate = MBPredicate.getStrPredicate(predicateStr, sentinel);
        this.isStrPredicate = true;
    }

    /**
     * @param countColumnName Column containing per-row counts
     * @param metricColumnName Column on which to classify outliers
     * @param meanColumnName Column containing means whose shifts will be explained
     * @param predicateStr Predicate used for classification: "==", "!=", "<", ">", "<=", or ">="
     * @param sentinel Double sentinel value used when evaluating the predicate to determine outlier
     */
    public CountMeanShiftCubedClassifier(
            final String countColumnName,
            final String metricColumnName,
            final String meanColumnName,
            final String predicateStr,
            final double sentinel
    ) throws MacroBaseException {
        super(countColumnName);
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
        String[] stringMetrics = input.getStringColumnByName(metricColumnName);
        double[] doubleMetrics = input.getDoubleColumnByName(metricColumnName);
        output = input.copy();
        double[] totalCountColumn = input.getDoubleColumnByName(getCountColumnName());
        double[] totalMeanColumn = input.getDoubleColumnByName(meanColumnName);
        int len = totalCountColumn.length;
        double[] outlierCountColumn = new double[len];
        double[] inlierCountColumn = new double[len];
        double[] outlierMeanSumColumn = new double[len];
        double[] inlierMeanSumColumn = new double[len];
        for (int i = 0; i < len; i++) {
                if ((isStrPredicate && strPredicate.test(stringMetrics[i])) ||
                        (!isStrPredicate && doublePredicate.test(doubleMetrics[i]))) {
                    outlierCountColumn[i] = totalCountColumn[i];
                    outlierMeanSumColumn[i] = totalMeanColumn[i] * totalCountColumn[i];
                } else {
                    inlierCountColumn[i] = totalCountColumn[i];
                    inlierMeanSumColumn[i] = totalMeanColumn[i] * totalCountColumn[i];
                }
        }
        output.addColumn(outlierCountColumnName, outlierCountColumn);
        output.addColumn(inlierCountColumnName, inlierCountColumn);
        output.addColumn(outlierMeanSumColumnName, outlierMeanSumColumn);
        output.addColumn(inlierMeanSumColumnName, inlierMeanSumColumn);
    }


    @Override
    public DataFrame getResults() {
        return output;
    }
}
