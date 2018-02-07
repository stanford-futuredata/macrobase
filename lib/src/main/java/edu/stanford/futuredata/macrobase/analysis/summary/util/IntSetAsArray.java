package edu.stanford.futuredata.macrobase.analysis.summary.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Small sets of integers. Fast to construct and compare equality, but does not
 * support checking for membership.
 */
public class IntSetAsArray implements IntSet {
    private int[] values;

    public IntSetAsArray(int a) {
        values = new int[1];
        values[0] = a;
    }

    public IntSetAsArray(IntSetAsLong newLong) {
        int a = newLong.getFirst();
        int b = newLong.getSecond();
        int c = newLong.getThird();
        if (b == 0) {
            values = new int[1];
            values[0] = a;
        } else if (c == 0) {
            values = new int[2];
            values[0] = b;
            values[1] = a;
        } else {
            values = new int[3];
            values[0] = c;
            values[1] = b;
            values[2] = a;
        }
    }

    public IntSetAsArray(int a, int b) {
        values = new int[2];
        if (a <= b) {
            values[0] = a;
            values[1] = b;
        } else {
            values[0] = b;
            values[1] = a;

        }
    }

    public IntSetAsArray(int a, int b, int c) {
        values = new int[3];
        values[0] = a;
        values[1] = b;
        values[2] = c;
    }

    /*
     * Hand-rolled three-integer sort.  Extremely performant and saves a lot of time in the
     * apriori/aplinear implementation versus just calling sort.
     */
    public IntSetAsArray(int a, int b, int c, HashMap<Integer, Integer> sortValues) {
        Integer keyA = sortValues.get(a);
        Integer keyB = sortValues.get(b);
        Integer keyC = sortValues.get(c);
        values = new int[3];
        if (keyA <= keyB) {
            if (keyA <= keyC) {
                values[0] = a;
                if (keyB <= keyC) {
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
            if (keyB <= keyC) {
                values[0] = b;
                if (keyA <= keyC) {
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

    public int getFirst() {
        return values[0];
    }

    public int getSecond() {
        return values[1];
    }

    public int getThird() {
        return values[2];
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IntSetAsArray that = (IntSetAsArray) o;

        return Arrays.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }

    @Override
    public String toString() {
        return "IntSetAsArray{" +
                "values=" + Arrays.toString(values) +
                '}';
    }
}
