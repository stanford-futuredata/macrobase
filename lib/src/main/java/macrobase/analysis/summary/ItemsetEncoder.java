package macrobase.analysis.summary;

import java.util.*;

public class ItemsetEncoder {
    private HashMap<Integer, Map<String, Integer>> encoder;
    private int nextKey;

    private HashMap<Integer, String> valueDecoder;
    private HashMap<Integer, Integer> columnDecoder;


    public ItemsetEncoder() {
        encoder = new HashMap<>();
        nextKey = 0;
        valueDecoder = new HashMap<>();
        columnDecoder = new HashMap<>();
    }

    public int decodeColumn(int i) {return columnDecoder.get(i);}
    public String decodeValue(int i) {return valueDecoder.get(i);}

    public List<Set<Integer>> encodeColumns(List<String[]> columns) {
        if (columns.isEmpty()) {
            return new ArrayList<>();
        }

        int d = columns.size();
        int numRows = columns.get(0).length;

        for (int i = 0; i < d; i++) {
            encoder.put(i, new HashMap<>());
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
