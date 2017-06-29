package edu.stanford.futuredata.macrobase.analysis.summary.groupby;

import java.util.HashMap;

public class DictionaryColumnEncoder {
    private HashMap<String, Integer> encoder;
    private String[] decoder;
    private int nextIdx = 0;

    public DictionaryColumnEncoder() {
        encoder = new HashMap<>();
        nextIdx = 0;
        decoder = null;
    }

    public int[] encodeColumn(String[] column) {
        int n = column.length;

        int[] encoded = new int[n];
        for (int i = 0; i < n; i++) {
            String curValue = column[i];
            if (!encoder.containsKey(curValue)) {
                encoder.put(curValue, nextIdx);
                encoded[i] = nextIdx;
                nextIdx++;
            } else {
                encoded[i] = encoder.get(curValue);
            }
        }

        decoder = new String[nextIdx];
        for (String curValue : encoder.keySet()) {
            int curIdx = encoder.get(curValue);
            decoder[curIdx] = curValue;
        }

        return encoded;
    }

    public String decode(int idx) {
        return decoder[idx];
    }

    public int encode(String curValue) {
        return encoder.get(curValue);
    }

    public int getNextIdx() {
        return nextIdx;
    }
}
