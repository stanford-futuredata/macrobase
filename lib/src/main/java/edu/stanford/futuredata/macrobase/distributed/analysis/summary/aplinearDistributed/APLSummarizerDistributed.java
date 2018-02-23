package edu.stanford.futuredata.macrobase.distributed.analysis.summary.aplinearDistributed;

import edu.stanford.futuredata.macrobase.analysis.summary.BatchSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLExplanation;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLExplanationResult;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.QualityMetric;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.distributed.analysis.summary.util.AttributeEncoderDistributed;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Generic summarizer superclass that can be customized with
 * different quality metrics and input sources. Subclasses are responsible
 * for converting from user-provided columns to the internal linear aggregates.
 */
public abstract class APLSummarizerDistributed extends BatchSummarizer {
    Logger log = LoggerFactory.getLogger("APLSummarizerDistributed");
    AttributeEncoderDistributed encoder;
    private APLExplanation explanation;
    protected JavaSparkContext sparkContext;
    protected int numPartitions = 1;

    protected long numEvents = 0;
    protected long numOutliers = 0;

    public abstract List<String> getAggregateNames();
    public abstract double[][] getAggregateColumns(DataFrame input);
    public abstract List<QualityMetric> getQualityMetricList();
    public abstract List<Double> getThresholds();
    public abstract JavaPairRDD<int[], double[]> getEncoded(
            JavaPairRDD<String[], double[]> partitionedDataFrame,
            double[] globalAggregates);

    APLSummarizerDistributed(JavaSparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }

    protected double[] processCountCol(DataFrame input, String countColumn, int numRows) {
        double[] countCol;
        if (countColumn != null) {
            countCol = input.getDoubleColumnByName(countColumn);
            for (int i = 0; i < numRows; i++) {
                numEvents += countCol[i];
            }
        } else {
            countCol = new double[numRows];
            for (int i = 0; i < numRows; i++) {
                countCol[i] = 1.0;
            }
            numEvents = numRows;
        }
        return countCol;
    }

    JavaPairRDD<String[], double[]> transformDataFrame(List<String[]> attributeColumns, double[][] aggregateColumns) {
        final int numAggregates = aggregateColumns.length;
        final int numRows = aggregateColumns[0].length;
        final int numColumns = attributeColumns.size();
        List<Tuple2<Integer, double[]>> aggregateRows = new ArrayList<>(numRows);
        for(int i = 0; i < numRows; i++) {
            double[] row = new double[numAggregates];
            for (int j = 0; j < numAggregates; j++) {
                row[j] = aggregateColumns[j][i];
            }
            aggregateRows.add(new Tuple2<>(i, row));
        }
        JavaPairRDD<Integer, double[]> aggregateRowsRDD =
                JavaPairRDD.fromJavaRDD(sparkContext.parallelize(aggregateRows, numPartitions));
        List<Tuple2<Integer, String[]>> attributeRows = new ArrayList<>(numRows);
        for(int i = 0; i < numRows; i++) {
            String[] row = new String[numColumns];
            for (int j = 0; j < numColumns; j++) {
                row[j] = attributeColumns.get(j)[i];
            }
            attributeRows.add(new Tuple2<>(i, row));
        }
        JavaPairRDD<Integer, String[]> attributeRowsRDD =
                JavaPairRDD.fromJavaRDD(sparkContext.parallelize(attributeRows, numPartitions));

        JavaPairRDD<Integer, Tuple2<String[], double[]>> mergedRdd =
                attributeRowsRDD.join(aggregateRowsRDD, numPartitions);

        JavaPairRDD<String[], double[]> mergedConsolidatedRDD =
                JavaPairRDD.fromJavaRDD(mergedRdd.map(
                        (Tuple2<Integer, Tuple2<String[], double[]>> entry) ->
                            new Tuple2<>(entry._2._1, entry._2._2)
        ));

        mergedConsolidatedRDD.cache();

        return mergedConsolidatedRDD;
    }

    public void process(DataFrame input) throws Exception {
        encoder = new AttributeEncoderDistributed();
        encoder.setColumnNames(attributes);
        long startTime = System.currentTimeMillis();
        JavaPairRDD<String[], double[]> partitionedDataFrame =
                transformDataFrame(input.getStringColsByName(attributes), getAggregateColumns(input));

        double[] globalAggregates = partitionedDataFrame.reduce(
                (Tuple2<String[], double[]> first, Tuple2<String[], double[]> second) -> {
            final int numAggregates = first._2.length;
            double[] sumAggregates = new double[numAggregates];
            for (int i = 0; i < numAggregates; i++)
                sumAggregates[i] = first._2[i] + second._2[i];
            return new Tuple2<>(first._1, sumAggregates);
        })._2;

        JavaPairRDD<int[], double[]> encoded = getEncoded(partitionedDataFrame, globalAggregates);
        long elapsed = System.currentTimeMillis() - startTime;
        log.info("Encoded in: {}", elapsed);
        log.info("Encoded Categories: {}", encoder.getNextKey() - 1);

        List<Double> thresholds = getThresholds();
        List<QualityMetric> qualityMetricList = getQualityMetricList();

        List<String> aggregateNames = getAggregateNames();
        List<APLExplanationResult> aplResults = APrioriLinearDistributed.explain(encoded,
                globalAggregates,
                encoder.getNextKey(),
                numPartitions,
                attributes.size(),
                qualityMetricList,
                thresholds
        );
        log.info("Number of results: {}", aplResults.size());

        explanation = new APLExplanation(
                encoder,
                Math.round(globalAggregates[1]),
                Math.round(globalAggregates[0]),
                aggregateNames,
                qualityMetricList,
                thresholds,
                aplResults
        );
    }

    public APLExplanation getResults() {
        return explanation;
    }

    public void setNumPartitions(int numPartitions) {this.numPartitions = numPartitions;}

}
