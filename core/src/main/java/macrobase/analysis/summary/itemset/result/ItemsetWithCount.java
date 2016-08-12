package macrobase.analysis.summary.itemset.result;

import java.util.Set;

public class ItemsetWithCount {
    private Set<Integer> items;
    private double count;

    public ItemsetWithCount(Set<Integer> items, double count) {
        this.items = items;
        this.count = count;
    }

    public Set<Integer> getItems() {
        return items;
    }

    public double getCount() {
        return count;
    }
}