package macrobase.analysis.summary.count;

import com.google.common.collect.Ordering;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 Maintains probabilistic heavy-hitters:
    - Item counts are overreported.
    - Once we have seen 1/threshold items, size is >= 1/threshold items.
 */
public class DirectCountWithThreshold extends ApproximateCount {
    private static final Logger log = LoggerFactory.getLogger(DirectCountWithThreshold.class);

    private HashMap<Integer, Double> counts = new HashMap<>();
    private double totalCount = 0;
    private final double threshold;
    private double minValue = 0;

    private final int minSize;

    // threshold == totalCount*threshold;
    public DirectCountWithThreshold(double threshold) {
        this.threshold = threshold;
        minSize = (int)(1/threshold);
    }

    @Override
    public void multiplyAllCounts(Double by) {
        minValue = Double.MAX_VALUE;

        totalCount *= by;

        // we're going to remove any extra items with count < threshold
        double filterCount = threshold*totalCount;

        List<Map.Entry<Integer, Double>> underweightCounts = new ArrayList<>();
        List<Double> underweightCountValues = new ArrayList<>();

        int overweightCount = 0;

        for(Map.Entry<Integer, Double> entry : counts.entrySet()) {
            double newValue = entry.getValue()*by;

            if(newValue >= filterCount) {
                overweightCount += 1;

                if(newValue < minValue) {
                    minValue = newValue;
                }
            } else {
                underweightCounts.add(entry);
                underweightCountValues.add(newValue);
            }

            counts.put(entry.getKey(), newValue);
        }

        if(overweightCount < minSize) {
            int needed = minSize-overweightCount;

            // just keep the underweight counts in the list
            // if we don't have enough
            if(needed >= underweightCounts.size()) {
                return;
            }

            // take the top elements in underweight counts and fill in remainder
            List<Double> threshValues = Ordering.natural().greatestOf(underweightCountValues, needed);

            double threshValue = threshValues.get(threshValues.size()-1);

            for(Map.Entry<Integer, Double> entry : underweightCounts) {
                if(entry.getValue() < threshValue) {
                    counts.remove(entry.getKey());
                } else {
                    counts.put(entry.getKey(), entry.getValue());
                    if(entry.getValue() < minValue) {
                        minValue = entry.getValue();
                    }
                }
            }
        } else {
            for(Map.Entry<Integer, Double> entry : underweightCounts) {
                counts.remove(entry.getKey());
            }
        }
    }

    public HashMap<Integer, Double> getCounts() {
        return counts;
    }

    @Override
    public void observe(Integer item, double count) {
        Double value = counts.get(item);
        if (value == null) {
            value = minValue + count;
        } else {
           value += count;
        }

        totalCount += value;

        counts.put(item, value);
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
