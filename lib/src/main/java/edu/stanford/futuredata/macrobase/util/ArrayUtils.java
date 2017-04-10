package edu.stanford.futuredata.macrobase.util;

public class ArrayUtils {
    public static double min(double[] a) {
        double minValue = Double.MAX_VALUE;
        int n = a.length;
        for (double curValue : a) {
            if (curValue < minValue) {
                minValue = curValue;
            }
        }
        return minValue;
    }
    public static double max(double[] a) {
        double maxValue = -Double.MAX_VALUE;
        int n = a.length;
        for (double curValue : a) {
            if (curValue > maxValue) {
                maxValue = curValue;
            }
        }
        return maxValue;
    }
}
