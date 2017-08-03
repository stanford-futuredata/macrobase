package edu.stanford.futuredata.macrobase.analysis.summary.fpg.result;

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

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        } else if (!(o instanceof ItemsetWithCount)) {
            return false;
        }
        final ItemsetWithCount other = (ItemsetWithCount) o;
        return (Math.round(other.getCount()) == Math.round(count)) && (other.getItems().equals(items));
    }
}