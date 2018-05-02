package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.analysis.classify.stats.MBPredicate;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.util.MacroBaseException;

import java.util.function.DoublePredicate;
import java.util.function.Predicate;

public class CountMeanShiftCubedClassifier extends CubeClassifier {
    private DoublePredicate predicate;
    private Predicate<String> strPredicate;
    private DoublePredicate doublePredicate;
    private DataFrame output;
    private String metricColumnName;
    private String meanColumnName;
    private boolean isStrPredicate;
    public static String outlierCountColumnName = "_OUTLIERCOUNT";
    public static String inlierCountColumnName = "_INLIERCOUNT";
    public static String outlierMeanColumnName = "_OUTLIERMEAN";
    public static String inlierMeanColumnName = "_INLIERMEAN";

    /**
     * @param metricColumnName Column on which to classifier outliers
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
     * @param metricColumnName Column on which to classifier outliers
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
     * remains unmodified; a copy is created and all modifications are made on the copy.
     * @throws Exception
     */
    @Override
    public void process(DataFrame input) throws Exception {
        String[] stringMetrics = input.getStringColumnByName(metricColumnName);
        double[] doubleMetrics = input.getDoubleColumnByName(metricColumnName);
        int len;
        if (isStrPredicate) {
            len = stringMetrics.length;
        } else {
            len = doubleMetrics.length;
        }
        output = input.copy();
        double[] totalCountColumn = input.getDoubleColumnByName(getCountColumnName());
        double[] totalMeanColumn = input.getDoubleColumnByName(meanColumnName);
        double[] outlierCountColumn = new double[len];
        double[] inlierCountColumn = new double[len];
        double[] outlierMeanColumn = new double[len];
        double[] inlierMeanColumn = new double[len];
        for (int i = 0; i < len; i++) {
            if (isStrPredicate) {
                final String curVal = stringMetrics[i];
                if (strPredicate.test(curVal)) {
                    outlierCountColumn[i] = totalCountColumn[i];
                    outlierMeanColumn[i] = totalMeanColumn[i] * totalCountColumn[i];
                } else {
                    inlierCountColumn[i] = totalCountColumn[i];
                    inlierMeanColumn[i] = totalMeanColumn[i] * totalCountColumn[i];
                }
            } else {
                final double curVal = doubleMetrics[i];
                if (doublePredicate.test(curVal)) {
                    outlierCountColumn[i] = totalCountColumn[i];
                    outlierMeanColumn[i] = totalMeanColumn[i] * totalCountColumn[i];
                } else {
                    inlierCountColumn[i] = totalCountColumn[i];
                    inlierMeanColumn[i] = totalMeanColumn[i] * totalCountColumn[i];
                }
            }
        }
        output.addColumn(outlierCountColumnName, outlierCountColumn);
        output.addColumn(inlierCountColumnName, inlierCountColumn);
        output.addColumn(outlierMeanColumnName, outlierMeanColumn);
        output.addColumn(inlierMeanColumnName, inlierMeanColumn);
    }


    @Override
    public DataFrame getResults() {
        return output;
    }
}
