package edu.stanford.futuredata.macrobase.distributed.analysis.classify;

import edu.stanford.futuredata.macrobase.analysis.classify.stats.MBPredicate;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.distributed.datamodel.DistributedDataFrame;
import edu.stanford.futuredata.macrobase.util.MacroBaseException;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.function.DoublePredicate;
import java.util.function.Predicate;

/**
 * PredicateClassifier classifies an outlier based on a predicate(e.g., equality, less than, greater than)
 * and a hard-coded sentinel value. Unlike PercentileClassifier, outlier values are not determined based on a
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
public class PredicateClassifierDistributed extends DistributedClassifier {

    private boolean isStrPredicate;
    private String predicateStr;
    private String stringSentinel;
    private double doubleSentinel;


    /**
     * @param columnName Column on which to classifier outliers
     * @param predicateStr Predicate used for classification: "==", "!=", "<", ">", "<=", or ">="
     * @param sentinel Sentinel value used when evaluating the predicate to determine outlier
     * @throws MacroBaseException
     */
    public PredicateClassifierDistributed(final String columnName, final String predicateStr, final double sentinel)
            throws MacroBaseException {
        super(columnName);
        this.doubleSentinel = sentinel;
        this.predicateStr = predicateStr;
        this.isStrPredicate = false;
    }


    /**
     * @param columnName Column on which to classifier outliers
     * @param predicateStr Predicate used for classification: "==", "!=", "<", ">", "<=", or ">="
     * @param sentinel Sentinel value used when evaluating the predicate to determine outlier
     * @throws MacroBaseException
     */
    public PredicateClassifierDistributed(final String columnName, final String predicateStr, final String sentinel)
            throws MacroBaseException {
        super(columnName);
        this.stringSentinel = sentinel;
        this.predicateStr = predicateStr;
        this.isStrPredicate = true;
    }


    /**
     * Scan through the metric column, and evaluate the predicate on every value in the column. The ``input'' DataFrame
     * remains unmodified; a copy is created and all modifications are made on the copy.
     * @throws Exception
     */
    @Override
    public DistributedDataFrame process(DistributedDataFrame input) throws Exception {
        if (isStrPredicate) {
            return processString(input);
        }
        else {
            return processDouble(input);
        }
    }

    private DistributedDataFrame processDouble(DistributedDataFrame input) throws Exception {
        int metricsIndex = input.getIndexOfColumn(columnName);
        JavaPairRDD<String[], double[]> outputRDD = input.dataFrameRDD.mapToPair(
                (Tuple2<String[], double[]> row) -> {
                    DoublePredicate predicate = MBPredicate.getDoublePredicate(predicateStr, doubleSentinel);
                    double[] oldDoubleRow = row._2;
                    double[] newDoubleRow = new double[oldDoubleRow.length + 1];
                    for (int i = 0; i < oldDoubleRow.length; i++) {
                        newDoubleRow[i] = oldDoubleRow[i];
                    }
                    final double curVal = row._2[metricsIndex];
                    if (predicate.test(curVal)) {
                        newDoubleRow[newDoubleRow.length - 1] = 1.0;
                    } else {
                        newDoubleRow[newDoubleRow.length - 1] = 0.0;
                    }
                    return new Tuple2<>(row._1, newDoubleRow);
                }
        );
        DistributedDataFrame output = new DistributedDataFrame(input.schema.copy(), outputRDD);
        output.addColumnToSchema(outputColumnName, Schema.ColType.DOUBLE);
        return output;
    }


    private DistributedDataFrame processString(DistributedDataFrame input) throws Exception {
        int metricsIndex = input.getIndexOfColumn(columnName);
        JavaPairRDD<String[], double[]> outputRDD = input.dataFrameRDD.mapToPair(
                (Tuple2<String[], double[]> row) -> {
                    Predicate<String> strPredicate = MBPredicate.getStrPredicate(predicateStr, stringSentinel);
                    double[] oldDoubleRow = row._2;
                    double[] newDoubleRow = new double[oldDoubleRow.length + 1];
                    for (int i = 0; i < oldDoubleRow.length; i++) {
                        newDoubleRow[i] = oldDoubleRow[i];
                    }
                    final String curVal = row._1[metricsIndex];
                    if (strPredicate.test(curVal)) {
                        newDoubleRow[newDoubleRow.length - 1] = 1.0;
                    } else {
                        newDoubleRow[newDoubleRow.length - 1] = 0.0;
                    }
                    return new Tuple2<>(row._1, newDoubleRow);
                }
        );
        DistributedDataFrame output = new DistributedDataFrame(input.schema.copy(), outputRDD);
        output.addColumnToSchema(outputColumnName, Schema.ColType.DOUBLE);
        return output;
    }
}
