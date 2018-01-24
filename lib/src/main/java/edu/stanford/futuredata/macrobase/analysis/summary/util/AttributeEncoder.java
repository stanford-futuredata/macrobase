package edu.stanford.futuredata.macrobase.analysis.summary.util;

import java.util.*;

/**
 * Encode every combination of attribute names and values into a distinct integer.
 * This class assumes that attributes are stored in String columns in dataframes
 * and is used inside of the explanation operators to search for explanatory
 * column values.
 */
public class AttributeEncoder {
    private HashMap<Integer, Map<String, Integer>> encoder;
    private int nextKey;

    private HashMap<Integer, String> valueDecoder;
    private HashMap<Integer, Integer> columnDecoder;
    private List<String> colNames;

    public AttributeEncoder() {
        encoder = new HashMap<>();
        nextKey = 0;
        valueDecoder = new HashMap<>();
        columnDecoder = new HashMap<>();
    }
    public void setColumnNames(List<String> colNames) {
        this.colNames = colNames;
    }

    public int decodeColumn(int i) {return columnDecoder.get(i);}
    public String decodeColumnName(int i) {return colNames.get(columnDecoder.get(i));}
    public String decodeValue(int i) {return valueDecoder.get(i);}

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
