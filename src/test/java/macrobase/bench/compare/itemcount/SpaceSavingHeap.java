package macrobase.bench.compare.itemcount;

import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import macrobase.analysis.summary.count.ApproximateCount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SpaceSavingHeap extends ApproximateCount {
    private static final Logger log = LoggerFactory.getLogger(SpaceSavingHeap.class);

    private class Counter implements Comparable<Counter> {
        public int item;
        public Double count;

        public Counter(int item, double count) {
            this.item = item;
            this.count = count;
        }

        @Override
        public int compareTo(Counter o) {
            return count.compareTo(o.count);
        }
    }

    Map<Integer, Counter> counterMap = new HashMap<>();
    MinMaxPriorityQueue<Counter> digest;
    private final int maxSize;
    private double totalCount;

    @Override
    public double getTotalCount() {
        return totalCount;
    }

    @Override
    public double getCount(int item) {
        if(counterMap.containsKey(item)) {
            return counterMap.get(item).count;
        }

        return digest.peekFirst().count;
    }

    @Override
    public void observe(Integer item, double count) {

        Counter c = counterMap.get(item);

        if(c == null) {
            if(counterMap.size() >= maxSize) {
                Counter remove = digest.pollFirst();
                counterMap.remove(remove.item);
                count += remove.count;
            }
            c = new Counter(item, count);
            counterMap.put(item, c);
            digest.add(c);
        } else {
            digest.remove(c);
            c.count += count;
            digest.add(c);
        }

        totalCount += count;

    }

    @Override
    public Map<Integer, Double> getCounts() {
        Map<Integer, Double> ret = Maps.newHashMap();
        Iterator<Counter> it= digest.iterator();
        while(it.hasNext()) {
            Counter c = it.next();
            ret.put(c.item, c.count);
        }
        return ret;
    }

    @Override
    public void multiplyAllCounts(Double by) {
        totalCount *= by;

        List<Counter> ret = new ArrayList<>();
        Iterator<Counter> it= digest.iterator();
        while(it.hasNext()) {
            Counter c = it.next();
            c.count *= by;
            ret.add(c);
        }

        digest.clear();

        for(Counter c : ret) {
            digest.add(c);
        }
    }

    public SpaceSavingHeap(int maxSize) {
        this.maxSize = maxSize;
        digest = MinMaxPriorityQueue.maximumSize(maxSize).<Counter>create();
    }
}