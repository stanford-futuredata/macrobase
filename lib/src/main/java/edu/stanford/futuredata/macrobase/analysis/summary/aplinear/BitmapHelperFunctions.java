package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.summary.util.*;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.AggregationOp;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class BitmapHelperFunctions {

    public static void updateAggregates(FastFixedHashTable thisThreadSetAggregates,
                                        IntSet curCandidate, AggregationOp[] aggregationOps,
                                  double[] val, int numAggregates) {
        double[] candidateVal = thisThreadSetAggregates.get(curCandidate);
        if (candidateVal == null) {
            thisThreadSetAggregates.put(curCandidate,
                    Arrays.copyOf(val, numAggregates));
        } else {
            for (int a = 0; a < numAggregates; a++) {
                AggregationOp curOp = aggregationOps[a];
                candidateVal[a] = curOp.combine(candidateVal[a], val[a]);
            }
        }
    }

    /*********************** All Order-2 helper methods ***********************/

    // One Bitmap, One Normal
    public static void allOneBitmapOneNormal(FastFixedHashTable thisThreadSetAggregates,
                                       ArrayList<Integer> outlierColList,
                                       AggregationOp[] aggregationOps, boolean[] singleNextArray,
                                       HashMap<Integer, RoaringBitmap>[] byThreadColumnBitmap,
                                       int[] curColumnTwoAttributes, int startIndex,
                                       boolean useIntSetAsArray, IntSet curCandidate, int numAggregates) {
        for (Integer curCandidateOne : outlierColList) {
            if (curCandidateOne == AttributeEncoder.noSupport || !singleNextArray[curCandidateOne])
                continue;
            if (byThreadColumnBitmap[1].containsKey(curCandidateOne)) {
                RoaringBitmap outlierBitmap = byThreadColumnBitmap[1].get(curCandidateOne);
                // pass in Array of [1, 1] for [outlier_count_col, total_count_col]
                oneBitmapOneNormal(thisThreadSetAggregates, outlierBitmap,
                        curCandidateOne, aggregationOps, singleNextArray,
                        curColumnTwoAttributes, startIndex, useIntSetAsArray,
                        curCandidate, new double[]{1, 1}, numAggregates);
            }
            if (byThreadColumnBitmap[0].containsKey(curCandidateOne)) {
                RoaringBitmap inlierBitmap = byThreadColumnBitmap[0].get(curCandidateOne);
                // pass in Array of [0, 1] for [outlier_count_col, total_count_col] (since this is for inliers)
                oneBitmapOneNormal(thisThreadSetAggregates, inlierBitmap,
                        curCandidateOne, aggregationOps, singleNextArray,
                        curColumnTwoAttributes, startIndex, useIntSetAsArray,
                        curCandidate, new double[]{0, 1}, numAggregates);
            }
        }
    }

    private static void oneBitmapOneNormal(FastFixedHashTable thisThreadSetAggregates,
                                    RoaringBitmap bitmap, Integer curCandidateOne,
                                    AggregationOp[] aggregationOps, boolean[] singleNextArray,
                                    int[] curColumnTwoAttributes, int startIndex,
                                    boolean useIntSetAsArray, IntSet curCandidate,
                                    double[] val, int numAggregates) {
        for (Integer rowNum : bitmap) {
            int rowNumInCol = rowNum - startIndex;
            if (curColumnTwoAttributes[rowNumInCol] == AttributeEncoder.noSupport ||
                    !singleNextArray[curColumnTwoAttributes[rowNumInCol]])
                continue;
            // Cascade to arrays if necessary, but otherwise pack attributes into longs.
            if (useIntSetAsArray) {
                curCandidate = new IntSetAsArray(curCandidateOne, curColumnTwoAttributes[rowNumInCol]);
            } else {
                ((IntSetAsLong) curCandidate).value = IntSetAsLong.twoIntToLong(curCandidateOne,
                        curColumnTwoAttributes[rowNumInCol]);
            }
            updateAggregates(thisThreadSetAggregates, curCandidate, aggregationOps, val, numAggregates);
        }
    }

    // Two Normal columns
    public static void allTwoNormal(FastFixedHashTable thisThreadSetAggregates,
                              int[] curColumnOneAttributes, int[] curColumnTwoAttributes,
                              AggregationOp[] aggregationOps, boolean[] singleNextArray,
                              int startIndex, int endIndex,
                              boolean useIntSetAsArray, IntSet curCandidate,
                              double[][] aRows, int numAggregates) {
        for (int rowNum = startIndex; rowNum < endIndex; rowNum++) {
            int rowNumInCol = rowNum - startIndex;
            // Only examine a pair if both its members have minimum support.
            if (curColumnOneAttributes[rowNumInCol] == AttributeEncoder.noSupport
                    || curColumnTwoAttributes[rowNumInCol] == AttributeEncoder.noSupport
                    || !singleNextArray[curColumnOneAttributes[rowNumInCol]]
                    || !singleNextArray[curColumnTwoAttributes[rowNumInCol]])
                continue;
            // Cascade to arrays if necessary, but otherwise pack attributes into longs.
            if (useIntSetAsArray) {
                curCandidate = new IntSetAsArray(curColumnOneAttributes[rowNumInCol],
                        curColumnTwoAttributes[rowNumInCol]);
            } else {
                ((IntSetAsLong) curCandidate).value = IntSetAsLong.twoIntToLong(curColumnOneAttributes[rowNumInCol],
                        curColumnTwoAttributes[rowNumInCol]);
            }
            updateAggregates(thisThreadSetAggregates, curCandidate, aggregationOps, aRows[rowNum], numAggregates);
        }
    }

    // Two bitmap columns
    public static void allTwoBitmap(FastFixedHashTable thisThreadSetAggregates,
                              ArrayList<Integer>[] outlierList,
                              AggregationOp[] aggregationOps, boolean[] singleNextArray,
                              HashMap<Integer, RoaringBitmap>[][] byThreadBitmap,
                              int colNumOne, int colNumTwo,
                              boolean useIntSetAsArray, IntSet curCandidate, int numAggregates) {
        for (Integer curCandidateOne : outlierList[colNumOne]) {
            if (curCandidateOne == AttributeEncoder.noSupport || !singleNextArray[curCandidateOne])
                continue;
            for (Integer curCandidateTwo : outlierList[colNumTwo]) {
                if (curCandidateTwo == AttributeEncoder.noSupport || !singleNextArray[curCandidateTwo])
                    continue;
                // Cascade to arrays if necessary, but otherwise pack attributes into longs.
                if (useIntSetAsArray) {
                    curCandidate = new IntSetAsArray(curCandidateOne, curCandidateTwo);
                } else {
                    ((IntSetAsLong) curCandidate).value = IntSetAsLong.twoIntToLong(curCandidateOne, curCandidateTwo);
                }
                int outlierCount = 0, inlierCount = 0;
                if (byThreadBitmap[colNumOne][1].containsKey(curCandidateOne) &&
                        byThreadBitmap[colNumTwo][1].containsKey(curCandidateTwo))
                    outlierCount = RoaringBitmap.andCardinality(byThreadBitmap[colNumOne][1].get(curCandidateOne),
                            byThreadBitmap[colNumTwo][1].get(curCandidateTwo));
                if (byThreadBitmap[colNumOne][0].containsKey(curCandidateOne) &&
                        byThreadBitmap[colNumTwo][0].containsKey(curCandidateTwo))
                    inlierCount = RoaringBitmap.andCardinality(byThreadBitmap[colNumOne][0].get(curCandidateOne),
                            byThreadBitmap[colNumTwo][0].get(curCandidateTwo));
                updateAggregates(thisThreadSetAggregates, curCandidate, aggregationOps,
                        new double[]{outlierCount, outlierCount + inlierCount}, numAggregates);
            }
        }
    }

    /*********************** All Order-3 helper methods ***********************/

    // One Bitmap, Two Normal
    public static void allOneBitmapTwoNormal(FastFixedHashTable thisThreadSetAggregates,
                                       ArrayList<Integer> outlierColList,
                                       AggregationOp[] aggregationOps, boolean[] singleNextArray,
                                       HashMap<Integer, RoaringBitmap>[] byThreadColumnBitmap,
                                       int[] curColumnTwoAttributes, int[] curColumnThreeAttributes, int startIndex,
                                       boolean useIntSetAsArray, IntSet curCandidate, int numAggregates) {
        for (Integer curCandidateOne : outlierColList) {
            if (curCandidateOne == AttributeEncoder.noSupport || !singleNextArray[curCandidateOne])
                continue;
            if (byThreadColumnBitmap[1].containsKey(curCandidateOne)) {
                RoaringBitmap outlierBitmap = byThreadColumnBitmap[1].get(curCandidateOne);
                // pass in Array of [1, 1] for [outlier_count_col, total_count_col]
                oneBitmapTwoNormal(thisThreadSetAggregates, outlierBitmap, curCandidateOne,
                        curColumnTwoAttributes, curColumnThreeAttributes, aggregationOps, singleNextArray,
                        startIndex, useIntSetAsArray, curCandidate, new double[]{1, 1}, numAggregates);
            }
            if (byThreadColumnBitmap[0].containsKey(curCandidateOne)) {
                RoaringBitmap inlierBitmap = byThreadColumnBitmap[0].get(curCandidateOne);
                // pass in Array of [0, 1] for [outlier_count_col, total_count_col] (since this is for the inliers)
                oneBitmapTwoNormal(thisThreadSetAggregates, inlierBitmap, curCandidateOne,
                        curColumnTwoAttributes, curColumnThreeAttributes, aggregationOps, singleNextArray,
                        startIndex, useIntSetAsArray, curCandidate, new double[]{0, 1}, numAggregates);
            }
        }
    }

    private static void oneBitmapTwoNormal(FastFixedHashTable thisThreadSetAggregates,
                                    RoaringBitmap bitmap, Integer curCandidateOne,
                                    int[] curColumnTwoAttributes, int[] curColumnThreeAttributes,
                                    AggregationOp[]  aggregationOps, boolean[] singleNextArray, int startIndex,
                                    boolean useIntSetAsArray, IntSet curCandidate,
                                    double[] val, int numAggregates) {
        for (Integer rowNum : bitmap) {
            int rowNumInCol = rowNum - startIndex;
            if (curColumnTwoAttributes[rowNumInCol] == AttributeEncoder.noSupport
                    || curColumnThreeAttributes[rowNumInCol] == AttributeEncoder.noSupport
                    || !singleNextArray[curColumnTwoAttributes[rowNumInCol]]
                    || !singleNextArray[curColumnThreeAttributes[rowNumInCol]])
                continue;
            // Cascade to arrays if necessary, but otherwise pack attributes into longs.
            if (useIntSetAsArray) {
                curCandidate = new IntSetAsArray(
                        curCandidateOne,
                        curColumnTwoAttributes[rowNumInCol],
                        curColumnThreeAttributes[rowNumInCol]);
            } else {
                ((IntSetAsLong) curCandidate).value = IntSetAsLong.threeIntToLong(
                        curCandidateOne,
                        curColumnTwoAttributes[rowNumInCol],
                        curColumnThreeAttributes[rowNumInCol]);
            }
            updateAggregates(thisThreadSetAggregates, curCandidate, aggregationOps, val, numAggregates);
        }
    }

    // Two Bitmaps, One Normal
    public static void allTwoBitmapsOneNormal(FastFixedHashTable thisThreadSetAggregates,
                                        ArrayList<Integer>[] outlierList,
                                        AggregationOp[] aggregationOps, boolean[] singleNextArray,
                                        HashMap<Integer, RoaringBitmap>[][] byThreadColumnBitmap,
                                        int colNumOne, int colNumTwo,
                                        int[] curColumnThreeAttributes, int startIndex,
                                        boolean useIntSetAsArray, IntSet curCandidate, int numAggregates) {
        for (Integer curCandidateOne : outlierList[colNumOne]) {
            if (curCandidateOne == AttributeEncoder.noSupport || !singleNextArray[curCandidateOne])
                continue;
            for (Integer curCandidateTwo : outlierList[colNumTwo]) {
                if (curCandidateTwo == AttributeEncoder.noSupport || !singleNextArray[curCandidateTwo])
                    continue;

                if (byThreadColumnBitmap[colNumOne][1].containsKey(curCandidateOne) &&
                        byThreadColumnBitmap[colNumTwo][1].containsKey(curCandidateTwo)) {
                    RoaringBitmap outlierBitmap = RoaringBitmap.and(
                            byThreadColumnBitmap[colNumOne][1].get(curCandidateOne),
                            byThreadColumnBitmap[colNumTwo][1].get(curCandidateTwo));
                    twoBitmapsOneNormal(thisThreadSetAggregates, outlierBitmap, curCandidateOne, curCandidateTwo,
                            curColumnThreeAttributes, aggregationOps, singleNextArray,
                            startIndex, useIntSetAsArray, curCandidate,
                            new double[]{1, 1}, numAggregates);
                }
                if (byThreadColumnBitmap[colNumOne][0].containsKey(curCandidateOne) &&
                        byThreadColumnBitmap[colNumTwo][0].containsKey(curCandidateTwo)) {
                    RoaringBitmap inlierBitmap = RoaringBitmap.and(
                            byThreadColumnBitmap[colNumOne][0].get(curCandidateOne),
                            byThreadColumnBitmap[colNumTwo][0].get(curCandidateTwo));
                    twoBitmapsOneNormal(thisThreadSetAggregates, inlierBitmap, curCandidateOne, curCandidateTwo,
                            curColumnThreeAttributes, aggregationOps, singleNextArray, startIndex, useIntSetAsArray,
                            curCandidate, new double[]{0, 1}, numAggregates);
                }
            }
        }
    }

    private static void twoBitmapsOneNormal(FastFixedHashTable thisThreadSetAggregates,
                                     RoaringBitmap bitmap, Integer curCandidateOne, Integer curCandidateTwo,
                                     int[] curColumnThreeAttributes,
                                     AggregationOp[]  aggregationOps, boolean[] singleNextArray, int startIndex,
                                     boolean useIntSetAsArray, IntSet curCandidate,
                                     double[] val, int numAggregates) {
        for (Integer rowNum : bitmap) {
            int rowNumInCol = rowNum - startIndex;
            if (curColumnThreeAttributes[rowNumInCol] == AttributeEncoder.noSupport ||
                    !singleNextArray[curColumnThreeAttributes[rowNumInCol]])
                continue;
            // Cascade to arrays if necessary, but otherwise pack attributes into longs.
            if (useIntSetAsArray) {
                curCandidate = new IntSetAsArray(
                        curCandidateOne,
                        curCandidateTwo,
                        curColumnThreeAttributes[rowNumInCol]);
            } else {
                ((IntSetAsLong) curCandidate).value = IntSetAsLong.threeIntToLong(
                        curCandidateOne,
                        curCandidateTwo,
                        curColumnThreeAttributes[rowNumInCol]);
            }
            updateAggregates(thisThreadSetAggregates, curCandidate, aggregationOps, val, numAggregates);
        }
    }

    // All Three Normal or All Three Bitmap
    public static void allThreeNormal(FastFixedHashTable thisThreadSetAggregates,
                                int[] curColumnOneAttributes, int[] curColumnTwoAttributes,
                                int[] curColumnThreeAttributes,
                                AggregationOp[]  aggregationOps, boolean[] singleNextArray,
                                int startIndex, int endIndex,
                                boolean useIntSetAsArray, IntSet curCandidate,
                                double[][] aRows, int numAggregates) {
        for (int rowNum = startIndex; rowNum < endIndex; rowNum++) {
            int rowNumInCol = rowNum - startIndex;
            // Only construct a triple if all its singleton members have minimum support.
            if (curColumnOneAttributes[rowNumInCol] == AttributeEncoder.noSupport
                    || curColumnTwoAttributes[rowNumInCol] == AttributeEncoder.noSupport
                    || curColumnThreeAttributes[rowNumInCol] == AttributeEncoder.noSupport
                    || !singleNextArray[curColumnThreeAttributes[rowNumInCol]]
                    || !singleNextArray[curColumnOneAttributes[rowNumInCol]]
                    || !singleNextArray[curColumnTwoAttributes[rowNumInCol]])
                continue;
            // Cascade to arrays if necessary, but otherwise pack attributes into longs.
            if (useIntSetAsArray) {
                curCandidate = new IntSetAsArray(
                        curColumnOneAttributes[rowNumInCol],
                        curColumnTwoAttributes[rowNumInCol],
                        curColumnThreeAttributes[rowNumInCol]);
            } else {
                ((IntSetAsLong) curCandidate).value = IntSetAsLong.threeIntToLong(
                        curColumnOneAttributes[rowNumInCol],
                        curColumnTwoAttributes[rowNumInCol],
                        curColumnThreeAttributes[rowNumInCol]);
            }
            updateAggregates(thisThreadSetAggregates, curCandidate, aggregationOps, aRows[rowNum], numAggregates);
        }
    }

    public static void allThreeBitmap(FastFixedHashTable thisThreadSetAggregates,
                                ArrayList<Integer>[] outlierList,
                                AggregationOp[]  aggregationOps, boolean[] singleNextArray,
                                HashMap<Integer, RoaringBitmap>[][] byThreadBitmap,
                                int colNumOne, int colNumTwo, int colNumThree,
                                boolean useIntSetAsArray, IntSet curCandidate, int numAggregates) {

        for (Integer curCandidateOne : outlierList[colNumOne]) {
            if (curCandidateOne == AttributeEncoder.noSupport || !singleNextArray[curCandidateOne])
                continue;
            for (Integer curCandidateTwo : outlierList[colNumTwo]) {
                if (curCandidateTwo == AttributeEncoder.noSupport || !singleNextArray[curCandidateTwo])
                    continue;
                for (Integer curCandidateThree : outlierList[colNumThree]) {
                    if (curCandidateThree == AttributeEncoder.noSupport || !singleNextArray[curCandidateThree])
                        continue;
                    // Cascade to arrays if necessary, but otherwise pack attributes into longs.
                    if (useIntSetAsArray) {
                        curCandidate = new IntSetAsArray(
                                curCandidateOne,
                                curCandidateTwo,
                                curCandidateThree);
                    } else {
                        ((IntSetAsLong) curCandidate).value = IntSetAsLong.threeIntToLong(
                                curCandidateOne,
                                curCandidateTwo,
                                curCandidateThree);
                    }
                    int outlierCount = 0, inlierCount = 0;
                    // index 1 is the outlier bitmap
                    if (byThreadBitmap[colNumOne][1].containsKey(curCandidateOne) &&
                            byThreadBitmap[colNumTwo][1].containsKey(curCandidateTwo) &&
                            byThreadBitmap[colNumThree][1].containsKey(curCandidateThree)) {
                        outlierCount = RoaringBitmap.andCardinality(
                                RoaringBitmap.and(byThreadBitmap[colNumOne][1].get(curCandidateOne),
                                        byThreadBitmap[colNumTwo][1].get(curCandidateTwo)),
                                byThreadBitmap[colNumThree][1].get(curCandidateThree));
                    }
                    if (byThreadBitmap[colNumOne][0].containsKey(curCandidateOne) &&
                            byThreadBitmap[colNumTwo][0].containsKey(curCandidateTwo) &&
                            byThreadBitmap[colNumThree][0].containsKey(curCandidateThree)) {
                        inlierCount = RoaringBitmap.andCardinality(
                                RoaringBitmap.and(byThreadBitmap[colNumOne][0].get(curCandidateOne),
                                        byThreadBitmap[colNumTwo][0].get(curCandidateTwo)),
                                byThreadBitmap[colNumThree][0].get(curCandidateThree));
                    }

                    updateAggregates(thisThreadSetAggregates, curCandidate, aggregationOps,
                            new double[]{outlierCount, outlierCount + inlierCount}, numAggregates);
                }
            }
        }
    }
}
