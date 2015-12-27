package macrobase.analysis.summary.count;

import macrobase.analysis.summary.result.DatumWithScore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DirectCountWithThreshold extends ApproximateCount {
    private HashMap<Integer, Double> counts = new HashMap<>();
    private double totalCount = 0;
    private final double threshold;

    // threshold == totalCount*threshold;
    public DirectCountWithThreshold(double threshold) {
        this.threshold = threshold;
    }


    @Override
    public void multiplyAllCounts(Double by) {
        totalCount *= by;

        double filterCount = threshold*totalCount;

        for(Map.Entry<Integer, Double> entry : counts.entrySet()) {
            // TODO: THRESHOLD THIS
            double newValue = entry.getValue()*by;

            if(newValue > filterCount) {
                counts.put(entry.getKey(), newValue);
            }
        }
    }

    public HashMap<Integer, Double> getCounts() {
        return counts;
    }

    @Override
    public void observe(Integer item, double count) {
        totalCount += count;
        Double curValue = counts.get(item);
        if (curValue == null) {
            counts.put(item, count);
        } else {
            counts.put(item, curValue + count);
        }
    }

    @Override
    public double getTotalCount() {
        return totalCount;
    }

    @Override
    public double getCount(int item) {
        Double ret = counts.get(item);
        if(ret == null) {
            return 0;
        }

        return ret;
    }
}
