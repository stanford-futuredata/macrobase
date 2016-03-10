package macrobase.analysis.summary.count;


import macrobase.datamodel.Datum;

import java.util.HashMap;
import java.util.List;

public class ExactCount {
    private HashMap<Integer, Double> counts = new HashMap<>();

    public HashMap<Integer, Double> getCounts() {
        return counts;
    }

    public ExactCount count(List<Datum> data) {
        for (Datum d : data) {
            for (int i : d.getAttributes()) {
                Double curVal = counts.get(i);
                if (curVal == null) {
                    curVal = 0.;
                }
                counts.put(i, curVal + 1);
            }
        }

        return this;
    }
}
