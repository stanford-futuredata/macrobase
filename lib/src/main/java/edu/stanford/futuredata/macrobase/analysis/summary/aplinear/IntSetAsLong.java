package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Sets of two or three integers of at most 20 bits each, stored as a long.
 * Extremely fast, but the integer size is capped and the integer must be nonzero.
 */
public class IntSetAsLong {

    /**
     * Pack two exponent-bit nonzero integers into a long in sorted order.
     * @param a  First integer
     * @param b  Second integer
     * @param exponent Number of bits in the integers
     * @return  A long containing both integers in the lowest exponent * 2 bits.
     */
    public static long twoIntToLong(long a, long b, long exponent) {
        if (a < b)
            return (a << exponent) + b;
        else
            return (b << exponent) + a;
    }

    /**
     * Pack three exponent-bit nonzero integers into a long in sorted order.
     * @param a  First integer
     * @param b  Second integer
     * @param c  Third integer
     * @param exponent Number of bits in the integers
     * @param sortValues values to sort by
     * @return  A long containing all integers in the lowest exponent * 3 bits.
     */
    public static long threeIntToLongSorted(long a, long b, long c, long exponent, HashMap<Integer, Integer> sortValues) {
        Integer keyA = sortValues.get(Math.toIntExact(a));
        Integer keyB = sortValues.get(Math.toIntExact(b));
        Integer keyC = sortValues.get(Math.toIntExact(c));
        long result = 0;
        long doubleExponent = exponent * 2;
        // Fast three-integer sort
        if (keyA <= keyB) {
            if (keyA <= keyC) {
                result += a << doubleExponent;
                if (keyB <= keyC) {
                    result += (b << exponent) + c;
                } else {
                    result += (c << exponent) + b;
                }
            } else {
                result = (c << doubleExponent) + (a << exponent) + b;
            }
        } else {
            if (keyB <= keyC) {
                result += b << doubleExponent;
                if (keyA <= keyC) {
                    result += (a << exponent) + c;
                } else {
                    result += (c << exponent) + a;
                }
            } else {
                result = (c << doubleExponent) + (b << exponent) + a;
            }
        }
        return result;
    }

    /**
     * Pack three exponent-bit nonzero integers into a long.
     * @param a  First integer
     * @param b  Second integer
     * @param c  Third integer
     * @param exponent Number of bits in the integers
     * @return  A long containing all integers in the lowest exponent * 3 bits.
     */
    public static long threeIntToLong(long a, long b, long c, long exponent) {
        return (a << (exponent * 2)) + (b << exponent) + c;
    }

    /**
     * Return the integer stored in the lowest exponent bits of newLong
     * @param newLong A long containing packed exponent-bit nonzero integers
     * @param exponent Number of bits in the integer
     * @return The exponent-bit integer stored in newLong's lowest exponent bits, 0 if none.
     */
    public static long getFirst(long newLong, long exponent) {
        return (newLong << (64 - exponent)) >>> (64 - exponent);
    }

    /**
     * Return the integer stored in the next-lowest exponent bits of newLong
     * @param newLong A long containing packed exponent-bit nonzero integers
     * @param exponent Number of bits in the integer
     * @return The exponent-bit integer stored in newLong's next exponent bits, 0 if none.
     */
    public static long getSecond(long newLong, long exponent) {
        return ((newLong >>> exponent) << (64 - exponent)) >>> (64 - exponent);
    }

    /**
     * Return the integer stored in the next-lowest exponent bits of newLong
     * @param newLong A long containing packed exponent-bit nonzero integers
     * @param exponent Number of bits in the integer
     * @return The exponent-bit integer stored in newLong's next exponent bits, 0 if none.
     */
    public static long getThird(long newLong, long exponent) {
        return newLong >>> (exponent * 2);
    }

    /**
     * Check if setLong contains queryLong
     * @param setLong A long containing packed exponent-bit nonzero integers
     * @param queryLong A exponent-bit integer
     * @param exponent Number of bits in the integer
     * @return Does setLong contain querylong?
     */
    public static boolean contains(long setLong, long queryLong, long exponent) {
        return getFirst(setLong, exponent) == queryLong
                || getSecond(setLong, exponent) == queryLong
                || getThird(setLong, exponent) == queryLong;
    }

    /**
     * Check if all ints in setLong are less than queryLong
     * @param setLong A long containing packed exponent-bit nonzero integers
     * @param queryLong A exponent-bit integer
     * @param exponent Number of bits in the integer
     * @return Are all ints in setLong less than queryLong?
     */
    public static boolean checkLessThan(long setLong, long queryLong, long exponent) {
        return (getFirst(setLong, exponent) < queryLong)
                && (getSecond(setLong, exponent) < queryLong)
                && (getThird(setLong, exponent) < queryLong);
    }

    /**
     * Check if all ints in setLong are less than the power of 2 used to create mask
     * @param setLong A long containing packed exponent-bit nonzero integers
     * @param mask A mask created by checkLessThanMaskCreate
     * @return Are all ints in setLong less than the power of 2 used to create the mask?
     */
    public static boolean checkLessThanMask(long setLong, long mask) {
        return (setLong & mask) == 0;
    }

    /**
     * Check if all ints in setLong are less than queryLong
     * @param queryLong A power of 2.  Must be greater than 2**exponent.
     * @param exponent Number of bits in the integers to be checked
     * @return A mask for use in checkLessThanMask
     */
    public static long checkLessThanMaskCreate(long queryLong, long exponent) {
        long mask = threeIntToLong(queryLong - 1, queryLong - 1, queryLong - 1, exponent);
        return ~mask;
    }

    /**
     * Return the nonzero integers stored in newLong
     * @param setLong A long containing packed exponent-bit nonzero integers
     * @param exponent Number of bits in the integer
     * @return A set of at most three integers stored in setLong.
     */
    public static Set<Integer> getSet(long setLong, long exponent) {
        HashSet<Integer> retSet = new HashSet<>(3);
        int first = Math.toIntExact(getFirst(setLong, exponent));
        retSet.add(first);
        int second = Math.toIntExact(getSecond(setLong, exponent));
        int third = Math.toIntExact(getThird(setLong, exponent));
        if (second != 0)
            retSet.add(second);
        if (third != 0)
            retSet.add(third);
        return retSet;
    }

    /**
     * Change the packing of setLong from oldExponent to newExponent.
     * @param setLong A long containing packed 20-bit nonzero integers.
     * @param newExponent The smallest x such that 2**x is greater than any of the numbers stored in setLong.
     * @param oldExponent Exponent used in original packing
     * @return A long with the new packing exponent
     */
    public static long changePacking(long setLong, long newExponent, long oldExponent) {
        return getFirst(setLong, oldExponent) + (getSecond(setLong, oldExponent) << newExponent) + (getThird(setLong, oldExponent) << (newExponent * 2));
    }

}
