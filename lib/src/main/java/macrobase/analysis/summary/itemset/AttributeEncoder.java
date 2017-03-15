package macrobase.analysis.summary.itemset;

import java.util.*;

/**
 * Encode every combination of attribute names and values into a distinct integer.
 * This class assumes that attributes are stored in String columns in dataframes
 * and is mainly used for frequent itemset mining.
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

    public List<Set<Integer>> encodeAttributes(List<String[]> columns) {
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

        ArrayList<Set<Integer>> encodedAttributes = new ArrayList<>(numRows);
        for (int i = 0; i < numRows; i++) {
            encodedAttributes.add(new HashSet<>());
        }

        for (int colIdx = 0; colIdx < d; colIdx++) {
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
                encodedAttributes.get(rowIdx).add(curKey);
            }
        }

        return encodedAttributes;
    }

}
