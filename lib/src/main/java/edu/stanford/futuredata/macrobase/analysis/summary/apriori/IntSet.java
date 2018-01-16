package edu.stanford.futuredata.macrobase.analysis.summary.apriori;

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
    /*
     * Hand-rolled three-integer sort.  Extremely performant and saves a lot of time in the
     * apriori/aplinear implementation versus just calling sort.
     */
    public IntSet(int a, int b, int c) {
        values = new int[3];
        if (a <= b) {
            if (a <= c) {
                values[0] = a;
                if (b <= c) {
                    values[1] = b;
                    values[2] = c;
                } else {
                    values[1] = c;
                    values[2] = b;
                }
            } else {
                values[0] = c;
                values[1] = a;
                values[2] = b;
            }
        } else {
            if (b <= c) {
                values[0] = b;
                if (a <= c) {
                    values[1] = a;
                    values[2] = c;
                } else {
                    values[1] = c;
                    values[2] = a;
                }
            } else {
                values[0] = c;
                values[1] = b;
                values[2] = a;
            }
        }
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

    public boolean contains(int i) {
        switch (values.length) {
            case 1: {
                return values[0] == 1;
            }
            case 2: {
                return values[0] == i || values[1] == i;
            }
            case 3: {
                return values[0] == i || values[1] == i || values[2] == i;
            }
            default: {
                return Arrays.binarySearch(values, i) >= 0;
            }
        }
    }

    public boolean contains(IntSet other) {
        int startIdx = 0;
        int n = values.length;
        for (int i : other.values) {
            startIdx = Arrays.binarySearch(values, startIdx, n, i);
            if (startIdx < 0) {
                return false;
            }
        }
        return true;
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
