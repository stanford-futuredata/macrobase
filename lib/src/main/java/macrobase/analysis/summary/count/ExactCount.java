package macrobase.analysis.summary.count;

import java.util.List;
import java.util.Set;
import java.util.HashMap;


public class ExactCount {
    private HashMap<Integer, Double> counts = new HashMap<>();

    public HashMap<Integer, Double> getCounts() {
        return counts;
    }

    public ExactCount count(List<Set<Integer>> transactions) {
        for (Set<Integer> txn : transactions) {
            for (int i : txn) {
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
