package macrobase.analysis.summary.count;

import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
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
            List<Map.Entry<Integer, Double>> a = Lists.newArrayList(counts.entrySet());
            a.sort((e1, e2) -> e1.getValue().compareTo(e2.getValue()));

            double prevVal = -1;
            int toRemove = counts.size() - maxStableSize;

            log.trace("Removing {} items from counts", toRemove);

            for(int i = 0; i < toRemove; ++i) {
                Map.Entry<Integer, Double> entry = a.get(i);
                counts.remove(entry.getKey());
                assert (prevVal < entry.getValue());
                prevVal = entry.getValue();
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
