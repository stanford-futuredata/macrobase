package edu.stanford.futuredata.macrobase.analysis.summary.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Sets of two or three integers of at most 31 or 21 bits each, stored as a long.
 * Extremely fast, but the integer size is capped and the integer must be nonzero.
 */
public class IntSetAsLong implements IntSet {

    public long value;

    public IntSetAsLong(long a) {
        this.value = a;
    }

    /**
     * Pack two 31-bit nonzero integers into a long in sorted order.
     * @param a  First integer.
     * @param b  Second integer.
     * @return  A long containing both integers in the lowest 62 bits.
     */
    public IntSetAsLong(long a, long b) {
        if (a < b)
            this.value = ((long) 1 << 62)  + (a << 31) + b;
        else
            this.value = ((long) 1 << 62)  + (b << 31) + a;
    }

    public static long twoIntToLong(long a, long b) {
        if (a < b)
            return ((long) 1 << 62)  + (a << 31) + b;
        else
            return ((long) 1 << 62)  + (b << 31) + a;
    }

    /**
     * Pack three 21-bit nonzero integers into a long in sorted order.
     * @param a  First integer.
     * @param b  Second integer.
     * @param c  Third integer.
     * @param sortValues Values by which to sort.
     * @return  A long containing all integers in the lowest 63 bits.
     */
    public IntSetAsLong(long a, long b, long c, HashMap<Integer, Integer> sortValues) {
        Integer keyA = sortValues.get(Math.toIntExact(a));
        Integer keyB = sortValues.get(Math.toIntExact(b));
        Integer keyC = sortValues.get(Math.toIntExact(c));
        long result = 0;
        // Fast three-integer sort
        if (keyA <= keyB) {
            if (keyA <= keyC) {
                result += a << 42;
                if (keyB <= keyC) {
                    result += (b << 21) + c;
                } else {
                    result += (c << 21) + b;
                }
            } else {
                result = (c << 42) + (a << 21) + b;
            }
        } else {
            if (keyB <= keyC) {
                result += b << 42;
                if (keyA <= keyC) {
                    result += (a << 21) + c;
                } else {
                    result += (c << 21) + a;
                }
            } else {
                result = (c << 42) + (b << 21) + a;
            }
        }
        this.value = result;
    }

    /**
     * Pack three 21-bit nonzero integers into a long.
     * @param a  First integer.
     * @param b  Second integer.
     * @param c  Third integer.
     * @return  A long containing all integers in the lowest  63 bits.
     */
    public IntSetAsLong(long a, long b, long c) {
        this.value = (a << (42)) + (b << 21) + c;
    }

    public static long threeIntToLong(long a, long b, long c) {
        if (b > c) {
           long temp = b;
           b = c;
           c = temp;
        }
        if (a > b) {
            long temp = a;
            a = b;
            b = temp;
        }
        if (b > c) {
            long temp = b;
            b = c;
            c = temp;
        }
        return (a << (42)) + (b << 21) + c;
    }

    /**
     * Return the integer stored in the lowest bits of newLong.
     * @return The integer stored in newLong's least-significant bits.
     */
    public int getFirst() {
        if (this.value >>> 62 == 1)
            return Math.toIntExact((this.value << (64 - 31)) >>> (64 - 31));
        else
            return Math.toIntExact((this.value << (64 - 21)) >>> (64 - 21));
    }

    /**
     * Return the integer stored in the next-lowest bits of newLong.
     * @return The integer stored in newLong's next least-significant bits.
     */
    public int getSecond() {
        if (this.value >>> 62 == 1)
            return Math.toIntExact(((this.value >>> 31) << (64 - 31)) >>> (64 - 31));
        else
            return Math.toIntExact(((this.value >>> 21) << (64 - 21)) >>> (64 - 21));
    }

    /**
     * Return the integer stored in the next-lowest bits of newLong.
     * @return The integer stored in newLong's most significant bits, 0 if none.
     */
    public int getThird() {
        if (this.value >>> 62 == 1)
            return 0;
        else
            return Math.toIntExact((this.value >>> 42));
    }

    /**
     * Check if setLong contains queryLong.
     * @param query An integer.
     * @return Does setLong contain querylong?
     */
    public boolean contains(int query) {
        return this.getFirst() == query
                || this.getSecond() == query
                || this.getThird() == query;
    }

    /**
     * Return the nonzero integers stored in newLong.
     * @return A set of at most three integers stored in setLong.
     */
    public Set<Integer> getSet() {
        HashSet<Integer> retSet = new HashSet<>(3);
        int first = this.getFirst();
        retSet.add(first);
        int second = this.getSecond();
        int third = this.getThird();
        if (second != 0)
            retSet.add(second);
        if (third != 0)
            retSet.add(third);
        return retSet;
    }

    @Override
    public boolean equals(Object o) {
        return ((IntSetAsLong) o).value == this.value;
    }

    @Override
    public int hashCode() {
        return (int) (this.value + 31 * (this.value >>> 11)  + 31 * (this.value >>> 22) + 7 * (this.value >>> 31)
                + (this.value >>> 45) + 31 * (this.value >>> 7) + 7 * (this.value >>> 37));
    }

}
