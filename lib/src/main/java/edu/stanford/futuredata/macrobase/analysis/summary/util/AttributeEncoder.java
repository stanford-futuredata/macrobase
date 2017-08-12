package edu.stanford.futuredata.macrobase.analysis.summary.util;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.Function;

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
    private HashMap<Integer, Function<String, String[]>> tokenizers;

    public AttributeEncoder() {
        encoder = new HashMap<>();
        nextKey = 0;
        valueDecoder = new HashMap<>();
        columnDecoder = new HashMap<>();
    }
    public AttributeEncoder setColumnNames(List<String> colNames) {
        this.colNames = colNames;
        return this;
    }

    public int decodeColumn(int i) {return columnDecoder.get(i);}
    public String decodeColumnName(int i) {return colNames.get(columnDecoder.get(i));}
    public String decodeValue(int i) {return valueDecoder.get(i);}

    public AttributeEncoder columnTokenizer(String columnName,
                                         Function<String, String[]> tokenizer) {
        assert(colNames != null && colNames.contains(columnName));
        if(tokenizers == null) {
            tokenizers = new HashMap<>();
        }

        tokenizers.put(colNames.indexOf(columnName), tokenizer);

        return this;
    }

    public List<int[]> encodeAttributes(List<String[]> columns) {
        if (columns.isEmpty()) {
            return new ArrayList<>();
        }

        int d = columns.size();
        int numRows = columns.get(0).length;

        for (int i = 0; i < d; i++) {
            if (!encoder.containsKey(i)) {
                encoder.put(i, new HashMap<>());
            }
        }

        ArrayList<int[]> encodedAttributes = new ArrayList<>(numRows);
        for (int i = 0; i < numRows; i++) {
            encodedAttributes.add(new int[d]);
        }

        for (int colIdx = 0; colIdx < d; colIdx++) {
            Map<String, Integer> curColEncoder = encoder.get(colIdx);
            String[] curCol = columns.get(colIdx);
            for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
                if (tokenizers == null || !tokenizers.containsKey(colIdx)) {
                    String colVal = curCol[rowIdx];
                    if (!curColEncoder.containsKey(colVal)) {
                        curColEncoder.put(colVal, nextKey);
                        valueDecoder.put(nextKey, colVal);
                        columnDecoder.put(nextKey, colIdx);
                        nextKey++;
                    }
                    int curKey = curColEncoder.get(colVal);
                    encodedAttributes.get(rowIdx)[colIdx] = curKey;
                } else {
                    for (String colVal : tokenizers.get(colIdx).apply(curCol[rowIdx])) {
                        if (!curColEncoder.containsKey(colVal)) {
                            curColEncoder.put(colVal, nextKey);
                            valueDecoder.put(nextKey, colVal);
                            columnDecoder.put(nextKey, colIdx);
                            nextKey++;
                        }
                        int curKey = curColEncoder.get(colVal);
                        encodedAttributes.get(rowIdx)[colIdx] = curKey;
                    }
                }
            }

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
