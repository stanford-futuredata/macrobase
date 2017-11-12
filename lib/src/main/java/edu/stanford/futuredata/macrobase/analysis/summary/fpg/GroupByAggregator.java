package edu.stanford.futuredata.macrobase.analysis.summary.fpg;

import edu.stanford.futuredata.macrobase.analysis.summary.fpg.result.ItemsetWithCount;

import java.util.*;

public class GroupByAggregator {
    public List<ItemsetWithCount> groupBy(List<Set<Integer>> items) {
        ArrayList<ItemsetWithCount> results = new ArrayList<>();
        HashMap<Set<Integer>, Integer> counts = new HashMap<>();
        for (Set<Integer> row : items) {
            if (!counts.containsKey(row))  {
                counts.put(row, 1);
            } else {
                counts.merge(row, 1, (x, y) -> x+y);
            }
        }
        for (Map.Entry<Set<Integer>, Integer> e : counts.entrySet()) {
            ItemsetWithCount ic = new ItemsetWithCount(
                    e.getKey(),
                    (double)e.getValue()
            );
            results.add(ic);
        }
        return results;
    }

    public List<ItemsetWithCount> groupByWithCounts(List<ItemsetWithCount> itemGroups) {
        ArrayList<ItemsetWithCount> results = new ArrayList<>();
        HashMap<Set<Integer>, Double> counts = new HashMap<>();
        for (ItemsetWithCount ic : itemGroups) {
            Set<Integer> s = ic.getItems();
            double curCount = ic.getCount();
            if (!counts.containsKey(s))  {
                counts.put(s, curCount);
            } else {
                counts.merge(s, curCount, (x, y) -> x+y);
            }
        }
        for (Map.Entry<Set<Integer>, Double> e : counts.entrySet()) {
            ItemsetWithCount ic = new ItemsetWithCount(
                    e.getKey(),
                    e.getValue()
            );
            results.add(ic);
        }
        return results;

    }
}
