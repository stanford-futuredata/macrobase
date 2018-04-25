package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.summary.util.*;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.AggregationOp;

import java.util.*;
import edu.stanford.futuredata.macrobase.analysis.summary.util.ModBitSet;

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
                              HashMap<Integer, ModBitSet>[][] byThreadBitmap,
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
                        byThreadBitmap[colNumTwo][1].containsKey(curCandidateTwo)) {
                    outlierCount = ModBitSet.andCardinality(byThreadBitmap[colNumOne][1].get(curCandidateOne),
                            byThreadBitmap[colNumTwo][1].get(curCandidateTwo));
                }
                if (byThreadBitmap[colNumOne][0].containsKey(curCandidateOne) &&
                        byThreadBitmap[colNumTwo][0].containsKey(curCandidateTwo)) {
                    inlierCount = ModBitSet.andCardinality(byThreadBitmap[colNumOne][0].get(curCandidateOne),
                            byThreadBitmap[colNumTwo][0].get(curCandidateTwo));
                }
                updateAggregates(thisThreadSetAggregates, curCandidate, aggregationOps,
                        new double[]{outlierCount, outlierCount + inlierCount}, numAggregates);
            }
        }
    }

    /*********************** All Order-3 helper methods ***********************/

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
                                HashMap<Integer, ModBitSet>[][] byThreadBitmap,
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
                        outlierCount = ModBitSet.andCardinality(byThreadBitmap[colNumOne][1].get(curCandidateOne),
                                byThreadBitmap[colNumTwo][1].get(curCandidateTwo),
                                byThreadBitmap[colNumThree][1].get(curCandidateThree));
                    }
                    if (byThreadBitmap[colNumOne][0].containsKey(curCandidateOne) &&
                            byThreadBitmap[colNumTwo][0].containsKey(curCandidateTwo) &&
                            byThreadBitmap[colNumThree][0].containsKey(curCandidateThree)) {
                        inlierCount = ModBitSet.andCardinality(byThreadBitmap[colNumOne][0].get(curCandidateOne),
                                byThreadBitmap[colNumTwo][0].get(curCandidateTwo),
                                byThreadBitmap[colNumThree][0].get(curCandidateThree));
                    }

                    updateAggregates(thisThreadSetAggregates, curCandidate, aggregationOps,
                            new double[]{outlierCount, outlierCount + inlierCount}, numAggregates);
                }
            }
        }
    }
}
