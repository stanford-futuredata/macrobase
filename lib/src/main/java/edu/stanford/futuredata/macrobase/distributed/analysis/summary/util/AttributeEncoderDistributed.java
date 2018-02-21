package edu.stanford.futuredata.macrobase.distributed.analysis.summary.util;

import edu.stanford.futuredata.macrobase.analysis.summary.util.AttributeEncoder;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

public class AttributeEncoderDistributed extends AttributeEncoder {

    public JavaPairRDD<Integer, int[]> encodeAttributesWithSupport(List<String[]> columns,
                                                          double minSupport,
                                                          double[] outlierColumn,
                                                          int distributedNumPartitions,
                                                          JavaSparkContext sparkContext) {

        int numColumns = columns.size();
        int numRows = columns.get(0).length;

        for (int i = 0; i < numColumns; i++) {
            if (!encoder.containsKey(i)) {
                encoder.put(i, new HashMap<>());
            }
        }
        // Create a map from strings to the number of times
        // each string appears in an outlier.
        int numOutliers = 0;
        HashMap<String, Double> countMap = new HashMap<>();
        for (int colIdx = 0; colIdx < numColumns; colIdx++) {
            String[] curCol = columns.get(colIdx);
            for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
                if (outlierColumn[rowIdx] > 0.0) {
                    if (colIdx == 0)
                        numOutliers += outlierColumn[rowIdx];
                    String colVal = Integer.toString(colIdx) + curCol[rowIdx];
                    Double curCount = countMap.get(colVal);
                    if (curCount == null)
                        countMap.put(colVal, outlierColumn[rowIdx]);
                    else
                        countMap.put(colVal, curCount + outlierColumn[rowIdx]);
                }
            }
        }

        // Rank the strings that have minimum support among the outliers
        // by the amount of support they have.
        double minSupportThreshold = minSupport * numOutliers;
        List<String> filterOnMinSupport= countMap.keySet().stream()
                .filter(line -> countMap.get(line) > minSupportThreshold)
                .collect(Collectors.toList());

        HashMap<String, Integer> stringToRank = new HashMap<>(filterOnMinSupport.size());
        for (int i = 0; i < filterOnMinSupport.size(); i++) {
            // We must one-index ranks because IntSetAsLong does not accept zero values.
            stringToRank.put(filterOnMinSupport.get(i), i + 1);
        }

        // Encode the strings that have support with a key equal to their rank.
        int[][] encodedAttributes = new int[numRows][numColumns];
        for (int colIdx = 0; colIdx < numColumns; colIdx++) {
            Map<String, Integer> curColEncoder = encoder.get(colIdx);
            String[] curCol = columns.get(colIdx);
            for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
                String colVal = curCol[rowIdx];
                String colNumAndVal = Integer.toString(colIdx) + colVal;
                if (!curColEncoder.containsKey(colVal)) {
                    if (stringToRank.containsKey(colNumAndVal)) {
                        int newKey = stringToRank.get(colNumAndVal);
                        curColEncoder.put(colVal, newKey);
                        valueDecoder.put(newKey, colVal);
                        columnDecoder.put(newKey, colIdx);
                        nextKey++;
                    } else {
                        curColEncoder.put(colVal, noSupport);
                    }
                }
                int curKey = curColEncoder.get(colVal);
                encodedAttributes[rowIdx][colIdx] = curKey;
            }
        }

        List<Tuple2<Integer, int[]>> encodedAttributesAsList = new ArrayList<>();
        for (int i = 0; i < encodedAttributes.length; i++) {
            encodedAttributesAsList.add(new Tuple2<>(i, encodedAttributes[i]));
        }
        JavaRDD<Tuple2<Integer, int[]>> encodedAttributesAsRDD = sparkContext.parallelize(encodedAttributesAsList, distributedNumPartitions);
        JavaPairRDD<Integer, int[]> encodeAttributesAsPairRDD = JavaPairRDD.fromJavaRDD(encodedAttributesAsRDD);

        return encodeAttributesAsPairRDD;
    }
}
