package edu.stanford.futuredata.macrobase.analysis.summary.util;

import java.util.*;
import java.util.stream.Collectors;

import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.AggregationOp;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.QualityMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encode every combination of attribute names and values into a distinct integer.
 * This class assumes that attributes are stored in String columns in dataframes
 * and is used inside of the explanation operators to search for explanatory
 * column values.
 */
public class AttributeEncoder {
    private Logger log = LoggerFactory.getLogger("AttributeEncoder");
    // An encoding for values which do not satisfy the minimum support threshold in encodeAttributesWithSupport.
    public static int noSupport = Integer.MAX_VALUE;
    public static int cardinalityThreshold = 128;

    private HashMap<Integer, Map<String, Integer>> encoder;
    private int nextKey;

    private HashMap<Integer, String> valueDecoder;
    private HashMap<Integer, Integer> columnDecoder;
    private List<String> colNames;
    private HashMap<Integer, ModBitSet>[][] bitmap;
    private int[] colCardinalities;
    private ArrayList<Integer> outlierList[];

    public AttributeEncoder() {
        encoder = new HashMap<>();
        // Keys must start at 1 because IntSetAsLong does not accept zero values.
        nextKey = 1;
        valueDecoder = new HashMap<>();
        columnDecoder = new HashMap<>();
    }
    public void setColumnNames(List<String> colNames) {
        this.colNames = colNames;
    }

    public int decodeColumn(int i) {return columnDecoder.get(i);}
    public String decodeColumnName(int i) {return colNames.get(columnDecoder.get(i));}
    public String decodeValue(int i) {return valueDecoder.get(i);}
    public HashMap<Integer, Integer> getColumnDecoder() {return columnDecoder;}
    public HashMap<Integer, ModBitSet>[][] getBitmap() {return bitmap;}
    public ArrayList<Integer>[] getOutlierList() {return outlierList;}
    public int[] getColCardinalities() {return colCardinalities;}

    /**
     * Encodes columns giving each value which satisfies a minimum support threshold a key
     * equal to its rank among all values which satisfy that threshold (so the single most common
     * value has key 1, the next has key 2, and so on).  Encode all values not satisfying the threshold
     * as AttributeEncoder.noSupport.
     * @param columns Columns to be encoded.
     * @param minSupport Minimum support to be satisfied.
     * @param outlierColumn The ith value in this array is the number of outliers whose attributes are those of
     *                      row i of columns.
     * @return A two-dimensional array of encoded values.
     */
    public int[][] encodeAttributesWithSupport(List<String[]> columns, boolean useBitmaps,
                                               int outlierColumnIdx,
                                               double[][] aggregateColumns,
                                               AggregationOp[] aggregationOps,
                                               List<QualityMetric> qualityMetrics,
                                               List<Double> thresholds) {
        if (columns.isEmpty()) {
            return new int[0][0];
        }

        int numColumns = columns.size();
        int numRows = columns.get(0).length;

        for (int i = 0; i < numColumns; i++) {
            if (!encoder.containsKey(i)) {
                encoder.put(i, new HashMap<>());
            }
        }
        int numAggregates = aggregateColumns.length;
        // Quality metrics are initialized with global aggregates to
        // allow them to determine the appropriate relative thresholds
        double[] globalAggregates = new double[numAggregates];
        for (int j = 0; j < numAggregates; j++) {
            AggregationOp curOp = aggregationOps[j];
            globalAggregates[j] = curOp.initValue();
            double[] curColumn = aggregateColumns[j];
            for (int i = 0; i < numRows; i++) {
                globalAggregates[j] = curOp.combine(globalAggregates[j], curColumn[i]);
            }
        }
        for (QualityMetric q : qualityMetrics) {
            q.initialize(globalAggregates);
        }
        // Row store for more convenient access
        final double[][] aRows = new double[numRows][numAggregates];
        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < numAggregates; j++) {
                aRows[i][j] = aggregateColumns[j][i];
            }
        }

        // Create a map from strings to aggregates
        HashMap<String, double[]> countMap = new HashMap<>();
        for (int colIdx = 0; colIdx < numColumns; colIdx++) {
            String[] curCol = columns.get(colIdx);
            for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
                // prepend column index as String to column value to disambiguate
                // between two identical values in different columns
                String colVal = Integer.toString(colIdx) + curCol[rowIdx];
                double[] candidateVal = countMap.get(colVal);
                if (candidateVal == null) {
                    countMap.put(colVal,
                            Arrays.copyOf(aRows[rowIdx], numAggregates));
                } else {
                    for (int a = 0; a < numAggregates; a++) {
                        AggregationOp curOp = aggregationOps[a];
                        candidateVal[a] = curOp.combine(candidateVal[a], aRows[rowIdx][a]);
                    }
                }
            }
        }

        // Filter strings that do not pass all monotonic quality metrics
        List<String> filterOnMinSupport = countMap.keySet().stream()
                .filter(line ->
                {
                    QualityMetric.Action action = QualityMetric.Action.KEEP;
                    for (int i = 0; i < qualityMetrics.size(); i++) {
                        QualityMetric q = qualityMetrics.get(i);
                        double t = thresholds.get(i);
                         action = QualityMetric.Action.combine(action, q.getAction(countMap.get(line), t));
                    }
                    return action == QualityMetric.Action.KEEP;
                })
                .collect(Collectors.toList());

        HashMap<String, Integer> stringToRank = new HashMap<>(filterOnMinSupport.size());
        for (int i = 0; i < filterOnMinSupport.size(); i++) {
            // We must one-index ranks because IntSetAsLong does not accept zero values.
            stringToRank.put(filterOnMinSupport.get(i), i + 1);
        }

        // Encode the strings that have support with a key equal to their rank.
        int[][] encodedAttributes = new int[numRows][numColumns];
        bitmap = new HashMap[numColumns][2];
        for (int i = 0; i < numColumns; ++i) {
            for (int j = 0; j < 2; j++)
                bitmap[i][j] = new HashMap<>();
        }
        outlierList = new ArrayList[numColumns];
        for (int i = 0; i < numColumns; i++)
            outlierList[i] = new ArrayList<>();
        colCardinalities = new int[numColumns];

        for (int colIdx = 0; colIdx < numColumns; colIdx++) {
            Map<String, Integer> curColEncoder = encoder.get(colIdx);
            String[] curCol = columns.get(colIdx);
            HashSet<Integer> foundOutliers = new HashSet<>();
            for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
                String colVal = curCol[rowIdx];
                // Again, prepend column index as String to column value to disambiguate
                // between two identical values in different columns
                String colNumAndVal = Integer.toString(colIdx) + colVal;
                int oidx = (aggregateColumns[outlierColumnIdx][rowIdx] > 0.0) ? 1 : 0; //1 = outlier, 0 = inlier
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

                if (oidx == 1 && curKey != noSupport && !foundOutliers.contains(curKey)) {
                    foundOutliers.add(curKey);
                    outlierList[colIdx].add(curKey);
                }
            }
            colCardinalities[colIdx] = outlierList[colIdx].size();
            if (!useBitmaps)
                colCardinalities[colIdx] = cardinalityThreshold + 1;
        }
        log.info("Column cardinalities: {}", Arrays.toString(colCardinalities));
        for (int colIdx = 0; colIdx < numColumns; colIdx++) {
            Map<String, Integer> curColEncoder = encoder.get(colIdx);
            String[] curCol = columns.get(colIdx);
            if (useBitmaps && colCardinalities[colIdx] < cardinalityThreshold) {
                for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
                    String colVal = curCol[rowIdx];
                    int oidx = (aggregateColumns[outlierColumnIdx][rowIdx] > 0.0) ? 1 : 0; //1 = outlier, 0 = inlier
                    int curKey = curColEncoder.get(colVal);
                    if (curKey != noSupport) {
                        if (bitmap[colIdx][oidx].containsKey(curKey)) {
                            bitmap[colIdx][oidx].get(curKey).set(rowIdx);
                        } else {
                            bitmap[colIdx][oidx].put(curKey, new ModBitSet());
                            bitmap[colIdx][oidx].get(curKey).set(rowIdx);
                        }
                    }
                }
            }
        }
        return encodedAttributes;
    }

    public int[][] encodeAttributesAsArray(List<String[]> columns) {
        if (columns.isEmpty()) {
            return new int[0][0];
        }

        int numColumns = columns.size();
        int numRows = columns.get(0).length;

        for (int i = 0; i < numColumns; i++) {
            if (!encoder.containsKey(i)) {
                encoder.put(i, new HashMap<>());
            }
        }

        colCardinalities = new int[numColumns];
        for (int i = 0; i < numColumns; i++)
            colCardinalities[i] = cardinalityThreshold + 1;

        int[][] encodedAttributes = new int[numRows][numColumns];

        for (int colIdx = 0; colIdx < numColumns; colIdx++) {
            Map<String, Integer> curColEncoder = encoder.get(colIdx);
            String[] curCol = columns.get(colIdx);
            for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
                String colVal = curCol[rowIdx];
                if (!curColEncoder.containsKey(colVal)) {
                    curColEncoder.put(colVal, nextKey);
                    valueDecoder.put(nextKey, colVal);
                    columnDecoder.put(nextKey, colIdx);
                    nextKey++;
                }
                int curKey = curColEncoder.get(colVal);
                encodedAttributes[rowIdx][colIdx] = curKey;
            }
        }

        return encodedAttributes;
    }

    public List<int[]> encodeAttributes(List<String[]> columns) {
        if (columns.isEmpty()) {
            return new ArrayList<>();
        }

        int[][] encodedArray = encodeAttributesAsArray(columns);
        int numRows = columns.get(0).length;

        ArrayList<int[]> encodedAttributes = new ArrayList<>(numRows);
        for (int i = 0; i < numRows; i++) {
            encodedAttributes.add(encodedArray[i]);
        }

        return encodedAttributes;
    }

    public List<Set<Integer>> encodeAttributesAsSets(List<String[]> columns) {
        List<int[]> arrays = encodeAttributes(columns);
        ArrayList<Set<Integer>> sets = new ArrayList<>(arrays.size());
        for (int[] row : arrays) {
            HashSet<Integer> curSet = new HashSet<>(row.length);
            for (int i : row) {
                curSet.add(i);
            }
            sets.add(curSet);
        }
        return sets;
    }

    public int getNextKey() {
        return nextKey;
    }

    public Map<String, String> decodeSet(Set<Integer> set) {
        HashMap<String, String> m = new HashMap<>(set.size());
        for (int i : set) {
            m.put(decodeColumnName(i), decodeValue(i));
        }
        return m;
    }

}
