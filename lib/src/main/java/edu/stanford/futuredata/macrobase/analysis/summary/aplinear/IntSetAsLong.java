package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import java.util.HashSet;
import java.util.Set;

/**
 * Sets of two or three integers of at most 20 bits each, stored as a long.
 * Extremely fast, but the integer size is capped and the integer must be nonzero.
 */
public class IntSetAsLong {
    private static long mask = 0xfffff;

    /**
     * Pack two twenty-bit nonzero integers into a long in sorted order.
     * @param a  First integer
     * @param b  Second integer
     * @return  A long containing both integers in the lowest 40 bits.
     */
    public static long twoIntToLong(long a, long b) {
        if (a < b)
            return (a << 20) + b;
        else
            return (b << 20) + a;
    }

    /**
     * Pack three twenty-bit nonzero integers into a long in sorted order.
     * @param a  First integer
     * @param b  Second integer
     * @param c  Third integer
     * @return  A long containing all integers in the lowest 60 bits.
     */
    public static long threeIntToLong(long a, long b, long c) {
        long result = 0;
        // Fast three-integer sort
        if (a <= b) {
            if (a <= c) {
                result += a << 40;
                if (b <= c) {
                    result += (b << 20) + c;
                } else {
                    result += (c << 20) + b;
                }
            } else {
                result = (c << 40) + (a << 20) + b;
            }
        } else {
            if (b <= c) {
                result += b << 40;
                if (a <= c) {
                    result += (a << 20) + c;
                } else {
                    result += (c << 20) + a;
                }
            } else {
                result = (c << 40) + (b << 20) + a;
            }
        }
        return result;
    }

    /**
     * Check if setLong contains queryLong
     * @param setLong A long containing packed 20-bit nonzero integers
     * @param queryLong A 20-bit integer
     * @return Does setLong contain querylong?
     */
    public static boolean contains(long setLong, long queryLong) {
        if ((setLong & mask) == queryLong)
            return true;
        else if (((setLong >>> 20) & mask) == queryLong)
            return true;
        else if (((setLong >>> 40) & mask) == queryLong)
            return true;
        else
            return false;
    }

    /**
     * Return the integer stored in the lowest 20 bits of newLong
     * @param newLong A long containing packed 20-bit nonzero integers
     * @return The 20-bit integer stored in newLong's lowest 20 bits, 0 if none.
     */
    public static long getFirst(long newLong) {
        return (newLong & mask);
    }

    /**
     * Return the integer stored in the next-lowest 20 bits of newLong
     * @param newLong A long containing packed 20-bit nonzero integers
     * @return The 20-bit integer stored in newLong's next 20 bits, 0 if none.
     */
    public static long getSecond(long newLong) {
        return ((newLong >>> 20) & mask);
    }

    /**
     * Return the integer stored in the next-lowest 20 bits of newLong
     * @param newLong A long containing packed 20-bit nonzero integers
     * @return The 20-bit integer stored in newLong's next 20 bits, 0 if none.
     */
    public static long getThird(long newLong) {
        return ((newLong >>> 40) & mask);
    }

    /**
     * Return the nonzero integers stored in newLong
     * @param setLong A long containing packed 20-bit nonzero integers
     * @return A set of at most three integers stored in setLong.
     */
    public static Set<Integer> getSet(long setLong) {
        HashSet<Integer> retSet = new HashSet(3);
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

    /**
     * Compacts setLong into the lowest-order bytes possible to create the smallest possible representation
     * of the numbers stored within.
     * @param setLong A long containing packed 20-bit nonzero integers.
     * @param exponent The smallest x such that 2**x is greater than any of the numbers stored in setLong.
     * @return A "packed" long with the integers stored in the lowest-order exponent*3 bits.
     */
    public static int hash(long setLong, long exponent) {
        return (int) (getFirst(setLong) + (getSecond(setLong) << exponent) + (getThird(setLong) << (exponent * 2)));
    }

}
