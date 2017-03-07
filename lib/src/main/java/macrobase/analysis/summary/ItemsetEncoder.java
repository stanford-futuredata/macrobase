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

    public List<Set<Integer>> encodeRows(List<String[]> rows) {
        if (rows.isEmpty()) {
            return new ArrayList<>();
        }
        String[] firstRow = rows.get(0);
        int d = firstRow.length;
        for (int i = 0; i < d; i++) {
            encoder.put(i, new HashMap<>());
        }

        ArrayList<Set<Integer>> encodedAttributes = new ArrayList<>(rows.size());
        for (int i = 0; i < rows.size(); i++) {
            encodedAttributes.add(new HashSet<>());
        }

        for (int colIdx = 0; colIdx < d; colIdx++) {
            Map<String, Integer> curColEncoder = encoder.get(colIdx);
            for (int i = 0; i < rows.size(); i++) {
                String[] curRow = rows.get(i);
                String colVal = curRow[colIdx];
                if (!curColEncoder.containsKey(colVal)) {
                    curColEncoder.put(colVal, nextKey);
                    valueDecoder.put(nextKey, colVal);
                    columnDecoder.put(nextKey, colIdx);
                    nextKey++;
                }
                int curKey = curColEncoder.get(colVal);
                encodedAttributes.get(i).add(curKey);
            }
        }

        return encodedAttributes;
    }
}
