package macrobase.analysis.summary.count;

import macrobase.analysis.summary.result.DatumWithScore;

import java.util.HashMap;
import java.util.List;

public class ExactCount {
    private HashMap<Integer, Integer> counts = new HashMap<>();

    public HashMap<Integer, Integer> getCounts() {
        return counts;
    }

    public ExactCount count(List<DatumWithScore> data) {
        for(DatumWithScore d : data) {
            for(int i : d.getDatum().getAttributes()) {
                Integer curVal = counts.get(i);
                if(curVal == null) {
                    curVal = 0;
                }
                counts.put(i, curVal+1);
            }
        }

        return this;
    }
}
