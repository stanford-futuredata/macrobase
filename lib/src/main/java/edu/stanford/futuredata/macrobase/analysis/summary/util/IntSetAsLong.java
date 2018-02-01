package edu.stanford.futuredata.macrobase.analysis.summary.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Sets of two or three integers of at most 31 or 21 bits each, stored as a long.
 * Extremely fast, but the integer size is capped and the integer must be nonzero.
 */
public class IntSetAsLong {

    /**
     * Pack two 31-bit nonzero integers into a long in sorted order.
     * @param a  First integer.
     * @param b  Second integer.
     * @return  A long containing both integers in the lowest 62 bits.
     */
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
    public static long threeIntToLongSorted(long a, long b, long c, HashMap<Integer, Integer> sortValues) {
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
        return result;
    }

    /**
     * Pack three 21-bit nonzero integers into a long.
     * @param a  First integer.
     * @param b  Second integer.
     * @param c  Third integer.
     * @return  A long containing all integers in the lowest  63 bits.
     */
    public static long threeIntToLong(long a, long b, long c) {
        return (a << (42)) + (b << 21) + c;
    }

    /**
     * Return the integer stored in the lowest bits of newLong.
     * @param newLong A long containing packed nonzero integers.
     * @return The integer stored in newLong's least-significant bits.
     */
    public static long getFirst(long newLong) {
        if (newLong >>> 62 == 1)
            return (newLong << (64 - 31)) >>> (64 - 31);
        else
            return (newLong << (64 - 21)) >>> (64 - 21);
    }

    /**
     * Return the integer stored in the next-lowest bits of newLong.
     * @param newLong A long containing packed nonzero integers.
     * @return The integer stored in newLong's next least-significant bits.
     */
    public static long getSecond(long newLong) {
        if (newLong >>> 62 == 1)
            return ((newLong >>> 31) << (64 - 31)) >>> (64 - 31);
        else
            return ((newLong >>> 21) << (64 - 21)) >>> (64 - 21);
    }

    /**
     * Return the integer stored in the next-lowest bits of newLong.
     * @param newLong A long containing packed nonzero integers.
     * @return The integer stored in newLong's most significant bits, 0 if none.
     */
    public static long getThird(long newLong) {
        if (newLong >>> 62 == 1)
            return 0;
        else
            return newLong >>> 42;
    }

    /**
     * Check if setLong contains queryLong.
     * @param setLong A long containing packed nonzero integers.
     * @param queryLong An integer.
     * @return Does setLong contain querylong?
     */
    public static boolean contains(long setLong, long queryLong) {
        return getFirst(setLong) == queryLong
                || getSecond(setLong) == queryLong
                || getThird(setLong) == queryLong;
    }

    /**
     * Return the nonzero integers stored in newLong.
     * @param setLong A long containing packed nonzero integers.
     * @return A set of at most three integers stored in setLong.
     */
    public static Set<Integer> getSet(long setLong) {
        HashSet<Integer> retSet = new HashSet<>(3);
        int first = Math.toIntExact(getFirst(setLong));
        retSet.add(first);
        int second = Math.toIntExact(getSecond(setLong));
        int third = Math.toIntExact(getThird(setLong));
        if (second != 0)
            retSet.add(second);
        if (third != 0)
            retSet.add(third);
        return retSet;
    }

}
