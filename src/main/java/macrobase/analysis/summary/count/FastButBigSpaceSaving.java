package macrobase.analysis.summary.count;

import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/*
 Maintains probabilistic heavy-hitters:
    - Item counts are overreported.
    - Once we have seen 1/threshold items, size is >= 1/threshold items.

 This is SpaceSaving but with:
   - O(1) update and O(items*log(k)) maintenance
     (normally O(log(k)) update)
   - unlimited space overhead within an epoch (normally O(k))

 Basic idea:
    Store min count from previous epoch
    Item in table? Increment
    Item not in table? Min + count

    End of epoch:
     1.) decay counts
     2.) record new min
     3.) compute 1/thresh highest counts
     4.) discard lower items, updating min if necessary

 */
public class FastButBigSpaceSaving extends ApproximateCount {
    private static final Logger log = LoggerFactory.getLogger(FastButBigSpaceSaving.class);

    private HashMap<Integer, Double> counts = new HashMap<>();
    private double totalCount = 0;
    private final double threshold;
    private final int maxStableSize;

    private double prevEpochMin = 0;

    // threshold == totalCount*threshold;
    public FastButBigSpaceSaving(double threshold) {
        this.threshold = threshold;
        maxStableSize = (int)(1/threshold);
    }

    @Override
    public void multiplyAllCounts(Double by) {
        totalCount *= by;

        log.trace("Decaying; {} items stored", counts.size());

        for(Map.Entry<Integer, Double> entry : counts.entrySet()) {
            double newValue = entry.getValue()*by;
            counts.put(entry.getKey(), newValue);

        }

        if(counts.size() > maxStableSize) {
            Set<Map.Entry<Integer, Double>> frozenEntrySet = Sets.newHashSet(counts.entrySet());

            // find the <maxStableSize>th value
            double threshValue = Ordering.natural().greatestOf(counts.values(), maxStableSize).get(maxStableSize-1);
            prevEpochMin = threshValue;

            log.trace("Pruning! Minimum threshold for count is {} ({})", threshValue, threshold);

            for(Map.Entry<Integer, Double> entry : frozenEntrySet) {
                if(entry.getValue() < threshValue) {
                    counts.remove(entry.getKey());
                }
            }
        }

        log.trace("Finished pruning; new size is {}", counts.size());
    }

    public HashMap<Integer, Double> getCounts() {
        return counts;
    }

    @Override
    public void observe(Integer item, double count) {
        Double value = counts.get(item);
        if (value == null) {
            value = prevEpochMin + count;
            totalCount += prevEpochMin + count;
        } else {
           value += count;
           totalCount += count;
        }

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
