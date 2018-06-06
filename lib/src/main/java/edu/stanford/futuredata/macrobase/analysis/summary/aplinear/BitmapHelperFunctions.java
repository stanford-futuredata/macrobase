package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.summary.util.*;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.AggregationOp;

import java.util.*;
import edu.stanford.futuredata.macrobase.analysis.summary.util.ModBitSet;

public class BitmapHelperFunctions {

    /**
     * Update aggregates during Apriori.
     * @param thisThreadSetAggregates A map from itemsets of attributes to arrays of aggregates.
     * @param curCandidate An itemset of attributes.
     * @param aggregationOps Aggregation functions used to perform updates.
     * @param aggregateVal The array of aggregates to be aggregated onto the entry for curCandidate
     * @param numAggregates The length of aggregateVal.
     */
    public static void updateAggregates(FastFixedHashTable thisThreadSetAggregates,
                                        IntSet curCandidate, AggregationOp[] aggregationOps,
                                  double[] aggregateVal, int numAggregates) {
        double[] candidateVal = thisThreadSetAggregates.get(curCandidate);
        if (candidateVal == null) {
            thisThreadSetAggregates.put(curCandidate,
                    Arrays.copyOf(aggregateVal, numAggregates));
        } else {
            for (int a = 0; a < numAggregates; a++) {
                AggregationOp curOp = aggregationOps[a];
                candidateVal[a] = curOp.combine(candidateVal[a], aggregateVal[a]);
            }
        }
    }

    /*********************** All Order-2 helper methods ***********************/

    /**
     * Iterate through two columns and update the map from attributes to aggregates
     * with all pairs of attributes found during iteration.  Process columns
     * without using bitmap representations.
     * @param thisThreadSetAggregates A map from itemsets of attributes to arrays of aggregates.
     * @param curColumnOneAttributes The first column of attributes.
     * @param curColumnTwoAttributes The second column of attributes.
     * @param aggregationOps Aggregation functions used to perform updates.
     * @param singleNextArray A list of supported singleton attributes.
     * @param startIndex Where to begin iteration in the columns.
     * @param endIndex Where to end iteration in the columns.  Only values between
     *                 startIndex and endIndex are considered.
     * @param useIntSetAsArray Whether candidates are to be stored in packed longs
     *                         or arrays of integers.
     * @param curCandidate A dummy IntSet used as a single-entry pool to speed up computation
     *                     by avoiding IntSet allocation.
     * @param aRows An array of aggregate values.
     * @param numAggregates The length of a row in aRows.
     */
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

    /**
     * Process two columns and update the map from attributes to aggregates
     * with all pairs of attributes found during iteration.  Process columns
     * using bitmap representations.
     * @param thisThreadSetAggregates A map from itemsets of attributes to arrays of aggregates.
     * @param outlierList A list whose entries are arrays of all attributes in
     *                    each column.
     * @param aggregationOps Aggregation functions used to perform updates.
     * @param singleNextArray A list of supported singleton attributes.
     * @param byThreadBitmap Bitmap representation of attributes.  Stored as array indexed
     *                       by column and then by outlier/inlier.  Each entry in array
     *                       is a map from encoded attribute value to the bitmap
     *                       for that attribute among outliers or inliers.
     * @param colNumOne The first column to process.
     * @param colNumTwo The second column to process.
     * @param useIntSetAsArray Whether candidates are to be stored in packed longs
     *                         or arrays of integers.
     * @param curCandidate A dummy IntSet used as a single-entry pool to speed up computation
     *                     by avoiding IntSet allocation.
     * @param numAggregates The length of a row in aRows.
     */
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
    /**
     * Iterate through three columns and update the map from attributes to aggregates
     * with all pairs of attributes found during iteration.  Process columns
     * without using bitmap representations.
     * @param thisThreadSetAggregates A map from itemsets of attributes to arrays of aggregates.
     * @param curColumnOneAttributes The first column of attributes.
     * @param curColumnTwoAttributes The second column of attributes.
     * @param curColumnThreeAttributes The third column of attributes.
     * @param aggregationOps Aggregation functions used to perform updates.
     * @param singleNextArray A list of supported singleton attributes.
     * @param startIndex Where to begin iteration in the columns.
     * @param endIndex Where to end iteration in the columns.  Only values between
     *                 startIndex and endIndex are considered.
     * @param useIntSetAsArray Whether candidates are to be stored in packed longs
     *                         or arrays of integers.
     * @param curCandidate A dummy IntSet used as a single-entry pool to speed up computation
     *                     by avoiding IntSet allocation.
     * @param aRows An array of aggregate values.
     * @param numAggregates The length of a row in aRows.
     */
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
    /**
     * Process three columns and update the map from attributes to aggregates
     * with all pairs of attributes found during iteration.  Process columns
     * using bitmap representations.
     * @param thisThreadSetAggregates A map from itemsets of attributes to arrays of aggregates.
     * @param outlierList A list whose entries are arrays of all attributes in
     *                    each column.
     * @param aggregationOps Aggregation functions used to perform updates.
     * @param singleNextArray A list of supported singleton attributes.
     * @param byThreadBitmap Bitmap representation of attributes.  Stored as array indexed
     *                       by column and then by outlier/inlier.  Each entry in array
     *                       is a map from encoded attribute value to the bitmap
     *                       for that attribute among outliers or inliers.
     * @param colNumOne The first column to process.
     * @param colNumTwo The second column to process.
     * @param colNumThree The third column to process.
     * @param useIntSetAsArray Whether candidates are to be stored in packed longs
     *                         or arrays of integers.
     * @param curCandidate A dummy IntSet used as a single-entry pool to speed up computation
     *                     by avoiding IntSet allocation.
     * @param numAggregates The length of a row in aRows.
     */
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
