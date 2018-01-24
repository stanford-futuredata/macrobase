package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import java.util.HashSet;
import java.util.Set;

public class IntSetAsLong {
    private static long mask = 0xfffff;

    public static long twoIntToLong(long a, long b) {
        if (a < b)
            return (a << 20) + b;
        else
            return (b << 20) + a;
    }

    public static long threeIntToLong(long a, long b, long c) {
        long result = 0;
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

    public static long getFirst(long newLong) {
        return (newLong & mask);
    }

    public static long getSecond(long newLong) {
        return ((newLong >>> 20) & mask);
    }

    public static long getThird(long newLong) {
        return ((newLong >>> 40) & mask);
    }

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

}
