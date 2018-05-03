package edu.stanford.futuredata.macrobase.distributed.analysis.summary.util;

import edu.stanford.futuredata.macrobase.analysis.summary.util.AttributeEncoder;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

public class AttributeEncoderDistributed extends AttributeEncoder {
    private Logger log = LoggerFactory.getLogger("AttributeEncoderDistributed");

    public JavaPairRDD<int[], double[]> encodeAttributesWithSupport(JavaPairRDD<String[], double[]> partitionedDataFrame,
                                                          int numColumns,
                                                          double minSupport,
                                                          int outlierColumnIndex, boolean useBitMaps) {

        for (int i = 0; i < numColumns; i++) {
            if (!encoder.containsKey(i)) {
                encoder.put(i, new HashMap<>());
            }
        }
        // Create a map from strings to the number of times
        // each string appears in an outlier.
        final String reservedOutlierString = "____RESERVED_OUTLIER_STRING_____";
        JavaRDD<HashMap<String, Double>> countMapRDD = partitionedDataFrame.mapPartitions((Iterator<Tuple2<String[], double[]>> iter) -> {
            HashMap<String, Double> countMap = new HashMap<>();
            countMap.put(reservedOutlierString, 0.0);
            while (iter.hasNext()) {
                Tuple2<String[], double[]> row = iter.next();
                double outlierCount = row._2[outlierColumnIndex];
                if (outlierCount > 0.0) {
                    countMap.put(reservedOutlierString, outlierCount + countMap.get(reservedOutlierString));
                    String[] rowAttributes = row._1;
                    for (int colIdx = 0; colIdx < rowAttributes.length; colIdx++) {
                        // Prepend column index as String to column value to disambiguate
                        // between two identical values in different columns
                        String colVal = Integer.toString(colIdx) + "," + rowAttributes[colIdx];
                        Double curCount = countMap.get(colVal);
                        if (curCount == null)
                            countMap.put(colVal, outlierCount);
                        else
                            countMap.put(colVal, curCount + outlierCount);
                    }
                }
            }
            List<HashMap<String, Double>> countMapList = new ArrayList<>(1);
            countMapList.add(countMap);
            return countMapList.iterator();
        }, true);
        HashMap<String, Double> countMap = countMapRDD.reduce((HashMap<String, Double> first, HashMap<String, Double> second) -> {
            HashMap<String, Double> returnTable = new HashMap<>();
            List<HashMap<String, Double>> tables = Arrays.asList(first, second);
            for (HashMap<String, Double> table: tables) {
                for (String key : table.keySet()) {
                    Double curCount = returnTable.get(key);
                    if (curCount == null)
                        returnTable.put(key, table.get(key));
                    else
                        returnTable.put(key, table.get(key) + curCount);
                }
            }
            return returnTable;
        });

        // Rank the strings that have minimum support among the outliers
        // by the amount of support they have.
        double numOutliers = countMap.get(reservedOutlierString);
        countMap.remove(reservedOutlierString);
        double minSupportThreshold = minSupport * numOutliers;
        outlierList = new ArrayList[numColumns];
        for (int i = 0; i < numColumns; i++)
            outlierList[i] = new ArrayList<>();
        colCardinalities = new int[numColumns];
        List<String> filterOnMinSupport = countMap.keySet().stream()
                .filter(line -> countMap.get(line) > minSupportThreshold)
                .collect(Collectors.toList());

        HashMap<String, Integer> stringToRank = new HashMap<>(filterOnMinSupport.size());
        for (int i = 0; i < filterOnMinSupport.size(); i++) {
            // We must one-index ranks because IntSetAsLong does not accept zero values.
            stringToRank.put(filterOnMinSupport.get(i), i + 1);
        }
        for (String key : stringToRank.keySet()) {
            int colIdx = Integer.parseInt(key.split(",", 2)[0]);
            String colVal = key.split(",", 2)[1];
            int newKey = stringToRank.get(key);
            outlierList[colIdx].add(newKey);
            Map<String, Integer> curColEncoder = encoder.get(colIdx);
            curColEncoder.put(colVal, newKey);
            valueDecoder.put(newKey, colVal);
            columnDecoder.put(newKey, colIdx);
            nextKey++;
        }

        for (int colIdx = 0; colIdx < numColumns; colIdx++) {
            colCardinalities[colIdx] = outlierList[colIdx].size();
            if (!useBitMaps)
                colCardinalities[colIdx] = cardinalityThreshold + 1;
        }

        // Encode the strings that have support with a key equal to their rank.
        JavaPairRDD<int[], double[]> encodedDataFrame = partitionedDataFrame.mapToPair((Tuple2<String[], double[]> entry) -> {
            String[] row = entry._1;
            int[] newRow = new int[numColumns];
            for (int colIdx = 0; colIdx < numColumns; colIdx++) {
                Map<String, Integer> curColEncoder = encoder.get(colIdx);
                String colVal = row[colIdx];
                int curKey;
                if (curColEncoder.containsKey(colVal))
                    curKey = curColEncoder.get(colVal);
                else
                    curKey = noSupport;
                newRow[colIdx] = curKey;
            }
            return new Tuple2<>(newRow, entry._2);
        });
        log.info("Column cardinalities: {}", Arrays.toString(colCardinalities));
        return encodedDataFrame;
    }
}
