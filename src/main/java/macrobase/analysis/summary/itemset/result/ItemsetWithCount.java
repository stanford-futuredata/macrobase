package macrobase.analysis.summary.itemset.result;

import java.util.Set;

public class ItemsetWithCount {
    private Set<Integer> items;
    private int count;

    public ItemsetWithCount(Set<Integer> items, int count) {
        this.items = items;
        this.count = count;
    }

    public Set<Integer> getItems() {
        return items;
    }

    public int getCount() {
        return count;
    }
}