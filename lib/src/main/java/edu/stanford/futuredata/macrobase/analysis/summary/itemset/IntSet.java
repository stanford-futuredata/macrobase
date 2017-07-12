package edu.stanford.futuredata.macrobase.analysis.summary.itemset;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Small sets of integers. Fast to construct and compare equality, but does not
 * support checking for membership.
 */
public class IntSet {
    public int[] values;

    public IntSet(int a) {
        values = new int[1];
        values[0] = a;
    }
    public IntSet(int a, int b) {
        values = new int[2];
        if (a <= b) {
            values[0] = a;
            values[1] = b;
        } else {
            values[0] = b;
            values[1] = a;

        }
    }
    public IntSet(int a, int b, int c) {
        values = new int[3];
        values[0] = a;
        values[1] = b;
        values[2] = c;
        Arrays.sort(values);
    }

    public IntSet(int[] values) {
        this.values = values.clone();
        Arrays.sort(this.values);
    }

    public int get(int idx) {
        return values[idx];
    }

    public int size() {
        return values.length;
    }

    public Set<Integer> getSet() {
        HashSet<Integer> curSet = new HashSet<>(values.length);
        for (int v : values) {
            curSet.add(v);
        }
        return curSet;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IntSet that = (IntSet) o;

        return Arrays.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }

    @Override
    public String toString() {
        return "IntSet{" +
                "values=" + Arrays.toString(values) +
                '}';
    }
}
