package edu.stanford.futuredata.macrobase.distributed.analysis.classify;

import edu.stanford.futuredata.macrobase.datamodel.Schema.ColType;
import edu.stanford.futuredata.macrobase.distributed.datamodel.DistributedDataFrame;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

public class PercentileClassifierDistributed extends DistributedClassifier {

  // Parameters
  private double percentile = 0.5;
  private boolean includeHigh = true;
  private boolean includeLow = true;

  // Calculated values
  private double lowCutoff;
  private double highCutoff;

  public PercentileClassifierDistributed(String columnName) {
    super(columnName);
  }

  @Override
  public DistributedDataFrame process(DistributedDataFrame input) {
    final int metricsIndex = input.getIndexOfColumn(columnName);
    // TODO: this is the naive solution: we collect all the doubles in the metric column on the
    // driver, then calculate the global percentile thresholds, and then broadcast the high/low
    // thresholds to the workers. Try Spark's approximate quantiles API to see if it gets good
    // enough accuracy
    final double[] metrics = input.dataFrameRDD.map((tuple) -> tuple._2[metricsIndex]).collect()
        .stream().mapToDouble((x) -> x).toArray();
    lowCutoff = new Percentile().evaluate(metrics, percentile);
    highCutoff = new Percentile().evaluate(metrics, 100.0 - percentile);
    final JavaPairRDD<String[], double[]> outputRDD = input.dataFrameRDD.mapToPair(
        (Tuple2<String[], double[]> row) -> {
          double[] oldDoubleRow = row._2;
          double[] newDoubleRow = new double[oldDoubleRow.length + 1];
          System.arraycopy(oldDoubleRow, 0, newDoubleRow, 0, oldDoubleRow.length);
          final double curVal = row._2[metricsIndex];
          if ((curVal > highCutoff && includeHigh) || (curVal < lowCutoff && includeLow)) {
            newDoubleRow[newDoubleRow.length - 1] = 1.0;
          }
          return new Tuple2<>(row._1, newDoubleRow);
        }
    );
    DistributedDataFrame output = new DistributedDataFrame(input.schema.copy(), outputRDD);
    output.addColumnToSchema(outputColumnName, ColType.DOUBLE);
    return output;
  }

  // Parameter Getters and Setters
  public double getPercentile() {
    return percentile;
  }

  /**
   * @param percentile Cutoff point for high or low values
   * @return this
   */
  public PercentileClassifierDistributed setPercentile(double percentile) {
    this.percentile = percentile;
    return this;
  }

  public boolean isIncludeHigh() {
    return includeHigh;
  }

  /**
   * @param includeHigh Whether to count high points as outliers.
   * @return this
   */
  public PercentileClassifierDistributed setIncludeHigh(boolean includeHigh) {
    this.includeHigh = includeHigh;
    return this;
  }

  public boolean isIncludeLow() {
    return includeLow;
  }

  /**
   * @param includeLow Whether to count low points as outliers
   * @return this
   */
  public PercentileClassifierDistributed setIncludeLow(boolean includeLow) {
    this.includeLow = includeLow;
    return this;
  }

  public double getLowCutoff() {
    return lowCutoff;
  }

  public double getHighCutoff() {
    return highCutoff;
  }
}
