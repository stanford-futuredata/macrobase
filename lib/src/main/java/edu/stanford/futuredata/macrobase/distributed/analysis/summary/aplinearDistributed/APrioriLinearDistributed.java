package edu.stanford.futuredata.macrobase.distributed.analysis.summary.aplinearDistributed;

import edu.stanford.futuredata.macrobase.analysis.summary.util.*;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.AggregationOp;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.QualityMetric;
import edu.stanford.futuredata.macrobase.util.MacroBaseInternalError;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLExplanationResult;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;

import static edu.stanford.futuredata.macrobase.analysis.summary.aplinear.BitmapHelperFunctions.*;

/**
 * Class for handling the generic, algorithmic aspects of apriori explanation.
 * This class assumes that subgroups posses "aggregates" such as count and outlier_count
 * which can be combined additively. Then, we use APriori to find the subgroups which
 * are the most interesting as defined by "quality metrics" on these aggregates.
 */
public class APrioriLinearDistributed {

    public static List<APLExplanationResult> explain(
            final JavaPairRDD<int[], double[]> attributesAndAggregates,
            double[] globalAggregates,
            int cardinality,
            int numPartitions,
            int numColumns,
            AggregationOp[] aggregationOps,
            ArrayList<Integer>[] outlierList,
            boolean[] isBitmapEncoded,
            List<QualityMetric> argQualityMetrics,
            List<Double> argThresholds
    ) {

        Logger log = LoggerFactory.getLogger("APLSummarizerDistributed");

        // Quality metrics and thresholds for candidate pruning and selection.
        QualityMetric[] qualityMetrics = argQualityMetrics.toArray(new QualityMetric[0]);
        double[] thresholds = new double[argThresholds.size()];
        for (int i = 0; i < argThresholds.size(); i++) {
            thresholds[i] = argThresholds.get(i);
        }
        // Singleton viable sets for quick lookup
        boolean[] singleNextArray = new boolean[cardinality];
        // Sets that have high enough support but not high qualityMetrics, need to be explored
        HashMap<Integer, HashSet<IntSet>> setNext = new HashMap<>(3);
        // Aggregate values for all of the sets we saved
        HashMap<Integer, Map<IntSet, double []>> savedAggregates = new HashMap<>(3);

        // Maximum order of explanations.
        final boolean useIntSetAsArray;
        // 2097151 is 2^21 - 1, the largest value that can fit in a length-three IntSetAsLong.
        // If the cardinality is greater than that, don't use them.
        if (cardinality >= 2097151) {
            log.warn("Cardinality is extremely high.  Candidate generation will be slow.");
            useIntSetAsArray = true;
        } else{
            useIntSetAsArray = false;
        }

        // Quality metrics are initialized with global aggregates to
        // allow them to determine the appropriate relative thresholds
        final int numRows = Math.toIntExact(attributesAndAggregates.count());
        final int numAggregates = globalAggregates.length;
        for (QualityMetric q : qualityMetrics) {
            q.initialize(globalAggregates);
        }

        // Shard the input RDD by rows, then store attribute information by column.
        // This allows easy distribution but also very fast processing.
        JavaRDD<Tuple2<Tuple2<int[][], double[][]>, HashMap<Integer, RoaringBitmap>[][]>> shardedAttributesAndAggregatesRDD =
                attributesAndAggregates.mapPartitions(
                        (Iterator<Tuple2<int[], double[]>> iter) -> {
            int[][] thisPartitionAttributes = new int[numColumns][(numRows + numPartitions * 100)/numPartitions];
            double[][] thisPartitionAggregates = new double[(numRows + numPartitions * 100)/numPartitions][numAggregates];
            int j = 0;
            while(iter.hasNext()) {
                Tuple2<int[], double[]> rowNext = iter.next();
                int[] attributesNext = rowNext._1;
                double[] aggregatesNext = rowNext._2;
                for (int i = 0; i < numColumns; i++) {
                    thisPartitionAttributes[i][j] = attributesNext[i];
                }
                for(int i = 0; i < numAggregates; i++) {
                    thisPartitionAggregates[j][i] = aggregatesNext[i];
                }
                j++;
            }
            int partitionNumRows = j;
            HashMap<Integer, RoaringBitmap>[][] partitionBitmaps = new HashMap[numColumns][2];
            for (int colIdx = 0; colIdx < numColumns; colIdx++) {
                for (int a = 0; a < 2; a++) {
                    partitionBitmaps[colIdx][a] = new HashMap<>();
                }
            }
            for (int colIdx = 0; colIdx < numColumns; colIdx++) {
                if (isBitmapEncoded[colIdx]) {
                    for (int rowIdx = 0; rowIdx < partitionNumRows; rowIdx++) {
                        int oidx = (thisPartitionAggregates[rowIdx][0] > 0.0) ? 1 : 0; //1 = outlier, 0 = inlier
                        int curKey = thisPartitionAttributes[colIdx][rowIdx];
                        if (curKey != AttributeEncoder.noSupport) {
                            if (partitionBitmaps[colIdx][oidx].containsKey(curKey)) {
                                partitionBitmaps[colIdx][oidx].get(curKey).add(rowIdx);
                            } else {
                                partitionBitmaps[colIdx][oidx].put(curKey, RoaringBitmap.bitmapOf(rowIdx));
                            }
                        }
                    }
                }
            }
            List<Tuple2<Tuple2<int[][], double[][]>, HashMap<Integer, RoaringBitmap>[][]>> returnList = new ArrayList<>(1);
            returnList.add(new Tuple2<>(new Tuple2<>(thisPartitionAttributes, thisPartitionAggregates), partitionBitmaps));
            return returnList.iterator();
        }, true);
        shardedAttributesAndAggregatesRDD.cache();

        for (int curOrder = 1; curOrder <= 3; curOrder++) {
            long startTime = System.currentTimeMillis();
            final int curOrderFinal = curOrder;
            // Do candidate generation in a lambda.
            JavaRDD<Map<IntSet, double[]>> hashTableSet = shardedAttributesAndAggregatesRDD.map((
                    Tuple2<Tuple2<int[][], double[][]>, HashMap<Integer, RoaringBitmap>[][]> sparkTuple) -> {
                int[][] attributesForThread = sparkTuple._1._1;
                double[][] aRowsForThread = sparkTuple._1._2;
                HashMap<Integer, RoaringBitmap>[][] partitionBitmaps = sparkTuple._2;
                FastFixedHashTable thisThreadSetAggregates = new FastFixedHashTable(cardinality, numAggregates, useIntSetAsArray);
                IntSet curCandidate;
                if (!useIntSetAsArray)
                    curCandidate = new IntSetAsLong(1, 1, 1);
                else
                    curCandidate = new IntSetAsArray(1, 1, 1);
                // Store global aggregates for partition
                double[] partitionAggregates = new double[numAggregates];
                for (int j = 0; j < numAggregates; j++) {
                    AggregationOp curOp = aggregationOps[j];
                    partitionAggregates[j] = curOp.initValue();
                }
                for (int i = 0; i < aRowsForThread.length; i++) {
                    for (int j = 0; j < numAggregates; j++) {
                        AggregationOp curOp = aggregationOps[j];
                        partitionAggregates[j] = curOp.combine(partitionAggregates[j], aRowsForThread[i][j]);
                    }
                }
                thisThreadSetAggregates.put(curCandidate, partitionAggregates);
                if (curOrderFinal == 1) {
                    for (int colNum = 0; colNum < numColumns; colNum++) {
                        if (isBitmapEncoded[colNum]) {
                            for (Integer curOutlierCandidate : outlierList[colNum]) {
                                // Require that all order-one candidates have minimum support.
                                if (curOutlierCandidate == AttributeEncoder.noSupport)
                                    continue;
                                int outlierCount = 0, inlierCount = 0;
                                if (partitionBitmaps[colNum][1].containsKey(curOutlierCandidate))
                                    outlierCount = partitionBitmaps[colNum][1].get(curOutlierCandidate).getCardinality();
                                if (partitionBitmaps[colNum][0].containsKey(curOutlierCandidate))
                                    inlierCount = partitionBitmaps[colNum][0].get(curOutlierCandidate).getCardinality();
                                // Cascade to arrays if necessary, but otherwise pack attributes into longs.
                                if (useIntSetAsArray) {
                                    curCandidate = new IntSetAsArray(curOutlierCandidate);
                                } else {
                                    ((IntSetAsLong) curCandidate).value = curOutlierCandidate;
                                }
                                updateAggregates(thisThreadSetAggregates, curCandidate, aggregationOps,
                                        new double[]{outlierCount, outlierCount + inlierCount}, numAggregates);
                            }
                        } else {
                            int[] curColumnAttributes = attributesForThread[colNum];
                            for (int rowNum = 0; rowNum < aRowsForThread.length; rowNum++) {
                                // Require that all order-one candidates have minimum support.
                                if (curColumnAttributes[rowNum] == AttributeEncoder.noSupport)
                                    continue;
                                // Cascade to arrays if necessary, but otherwise pack attributes into longs.
                                if (useIntSetAsArray) {
                                    curCandidate = new IntSetAsArray(curColumnAttributes[rowNum]);
                                } else {
                                    ((IntSetAsLong) curCandidate).value = curColumnAttributes[rowNum];
                                }
                                updateAggregates(thisThreadSetAggregates, curCandidate, aggregationOps,
                                        aRowsForThread[rowNum], numAggregates);
                            }
                        }
                    }
                } else if (curOrderFinal == 2) {
                    for (int colNumOne = 0; colNumOne < numColumns; colNumOne++) {
                        int[] curColumnOneAttributes = attributesForThread[colNumOne];
                        for (int colNumTwo = colNumOne + 1; colNumTwo < numColumns; colNumTwo++) {
                            int[] curColumnTwoAttributes = attributesForThread[colNumTwo];

                            if (isBitmapEncoded[colNumOne] && isBitmapEncoded[colNumTwo]) {
                                // Bitmap-Bitmap
                                allTwoBitmap(thisThreadSetAggregates, outlierList, aggregationOps, singleNextArray,
                                        partitionBitmaps, colNumOne, colNumTwo, useIntSetAsArray,
                                        curCandidate, numAggregates);
                            } else {
                                // Normal-Normal
                                allTwoNormal(thisThreadSetAggregates, curColumnOneAttributes,
                                        curColumnTwoAttributes, aggregationOps, singleNextArray,
                                        0,  aRowsForThread.length, useIntSetAsArray, curCandidate, aRowsForThread,
                                        numAggregates);
                            }
                        }
                    }
                } else if (curOrderFinal == 3) {
                    for (int colNumOne = 0; colNumOne < numColumns; colNumOne++) {
                        int[] curColumnOneAttributes = attributesForThread[colNumOne % numColumns];
                        for (int colNumTwo = colNumOne + 1; colNumTwo < numColumns; colNumTwo++) {
                            int[] curColumnTwoAttributes = attributesForThread[colNumTwo % numColumns];
                            for (int colNumThree = colNumTwo + 1; colNumThree < numColumns; colNumThree++) {
                                int[] curColumnThreeAttributes = attributesForThread[colNumThree % numColumns];
                                if (isBitmapEncoded[colNumOne] && isBitmapEncoded[colNumTwo] &&
                                        isBitmapEncoded[colNumThree]) {
                                    // all 3 cols are bitmaps
                                    allThreeBitmap(thisThreadSetAggregates, outlierList, aggregationOps,
                                            singleNextArray, partitionBitmaps,
                                            colNumOne, colNumTwo, colNumThree, useIntSetAsArray, curCandidate,
                                            numAggregates);

                                } else {
                                    // all three are normal
                                    allThreeNormal(thisThreadSetAggregates, curColumnOneAttributes,
                                            curColumnTwoAttributes, curColumnThreeAttributes,
                                            aggregationOps, singleNextArray, 0, aRowsForThread.length,
                                            useIntSetAsArray, curCandidate, aRowsForThread, numAggregates);
                                }
                            }
                        }
                    }
                } else {
                    throw new MacroBaseInternalError("High Order not supported");
                }
                return thisThreadSetAggregates.asHashMap();
            });

            /*hashTableSet.cache();

            // The collected candidate hash tables can be very large, so we prune them before reducing in order
            // to minimize the amount of data transfer and serial computation.  We prune by identifying the set of all
            // candidates that pass the support threshold on any partition, then filtering out all other candidates
            // on every partition.
            JavaRDD<HashSet<IntSet>> prunedHashTableSet = hashTableSet.map((Map<IntSet, double[]> setAggregates) -> {
                HashSet<IntSet> thisThreadPassingAggregates = new HashSet<>();
                double[] partitionAggregates;
                partitionAggregates = setAggregates.get(new IntSetAsArray(1, 1, 1));
                QualityMetric[] partitionQualityMetrics = argQualityMetrics.toArray(new QualityMetric[0]);
                for (QualityMetric q : partitionQualityMetrics) {
                    q.initialize(partitionAggregates);
                }
                for (IntSet curCandidateSet: setAggregates.keySet()) {
                    double[] curAggregates = setAggregates.get(curCandidateSet);
                    boolean canPassThreshold = true;
                    for (int i = 0; i < partitionQualityMetrics.length; i++) {
                        QualityMetric q = partitionQualityMetrics[i];
                        double t = thresholds[i];
                        canPassThreshold &= q.maxSubgroupValue(curAggregates) >= t;
                    }
                    if (canPassThreshold) {
                        thisThreadPassingAggregates.add(curCandidateSet);
                    }
                }
                return thisThreadPassingAggregates;
            });

            // This is the set of all candidates that pass the support threshold on any partition.
            final HashSet<IntSet> combinedPrunedHashTableSet = prunedHashTableSet.reduce((HashSet<IntSet> one, HashSet<IntSet> two) -> {
                HashSet<IntSet> combined = new HashSet<>();
                combined.addAll(one);
                combined.addAll(two);
                return combined;
            });

            // Remove all candidates not in combinedPrunedHashTableSet.
            JavaRDD<Map<IntSet, double[]>> finalPrunedHashTableSet = hashTableSet.map((Map<IntSet, double[]> setAggregates) -> {
                Map<IntSet, double[]> thisThreadPassingAggregates = new HashMap<>();
                for (IntSet curCandidate : setAggregates.keySet()) {
                    double[] curAggregates = setAggregates.get(curCandidate);
                    if (combinedPrunedHashTableSet.contains(curCandidate)) {
                        thisThreadPassingAggregates.put(curCandidate, curAggregates);
                    }
                }
                return thisThreadPassingAggregates;
            });*/

            // Finally, reduce over the pruned hash tables to get a final candidate and counts map..
            Map<IntSet, double[]> setAggregates =
                    hashTableSet.reduce((Map<IntSet, double[]> tableOne, Map<IntSet, double[]> tableTwo) -> {
                        List<Map<IntSet, double[]>> tables = Arrays.asList(tableOne, tableTwo);
                        Map<IntSet, double[]> tableCombined = new HashMap<>();
                        for (Map<IntSet, double[]> table : tables) {
                            for (IntSet curCandidateKey : table.keySet()) {
                                double[] curCandidateValue = table.get(curCandidateKey);
                                double[] candidateVal = tableCombined.get(curCandidateKey);
                                if (candidateVal == null) {
                                    tableCombined.put(curCandidateKey, Arrays.copyOf(curCandidateValue, numAggregates));
                                } else {
                                    for (int a = 0; a < numAggregates; a++) {
                                        AggregationOp curOp = aggregationOps[a];
                                        candidateVal[a] = curOp.combine(candidateVal[a], curCandidateValue[a]);
                                    }
                                }
                            }
                        }
                        return tableCombined;
                    }
            );

            // Prune all the collected aggregates
            HashSet<IntSet> curOrderNext = new HashSet<>();
            HashSet<IntSet> curOrderSaved = new HashSet<>();
            for (IntSet curCandidate: setAggregates.keySet()) {
                QualityMetric.Action action = QualityMetric.Action.KEEP;
                if (curOrder == 1 && curCandidate.getFirst() == AttributeEncoder.noSupport) {
                    action = QualityMetric.Action.PRUNE;
                } else {
                    double[] curAggregates = setAggregates.get(curCandidate);
                    for (int i = 0; i < qualityMetrics.length; i++) {
                        QualityMetric q = qualityMetrics[i];
                        double t = thresholds[i];
                        action = QualityMetric.Action.combine(action, q.getAction(curAggregates, t));
                    }
                    if (action == QualityMetric.Action.KEEP) {
                        // Make sure the candidate isn't already covered by a pair
                        if (curOrder != 3 || validateCandidate(curCandidate, setNext.get(2))) {
                            // if a set is already past the threshold on all metrics,
                            // save it and no need for further exploration if we do containment
                            curOrderSaved.add(curCandidate);
                        }
                    } else if (action == QualityMetric.Action.NEXT) {
                        // otherwise if a set still has potentially good subsets,
                        // save it for further examination
                        curOrderNext.add(curCandidate);
                    }
                }
            }

            // Save aggregates that pass all qualityMetrics to return later, store aggregates
            // that have minimum support for higher-order exploration.
            Map<IntSet, double []> curSavedAggregates = new HashMap<>(curOrderSaved.size());
            for (IntSet curSaved : curOrderSaved) {
                curSavedAggregates.put(curSaved, setAggregates.get(curSaved));
            }
            savedAggregates.put(curOrder, curSavedAggregates);
            setNext.put(curOrder, curOrderNext);
            if (curOrder == 1) {
                for (IntSet i : curOrderNext) {
                    singleNextArray[i.getFirst()] = true;
                }
            }
            log.info("Time spent in order {}:  {}",
                    curOrderFinal, System.currentTimeMillis() - startTime);
        }

        List<APLExplanationResult> results = new ArrayList<>();
        for (int curOrder: savedAggregates.keySet()) {
            Map<IntSet, double []> curOrderSavedAggregates = savedAggregates.get(curOrder);
            for (IntSet curSet : curOrderSavedAggregates.keySet()) {
                double[] aggregates = curOrderSavedAggregates.get(curSet);
                double[] metrics = new double[qualityMetrics.length];
                for (int i = 0; i < metrics.length; i++) {
                    metrics[i] = qualityMetrics[i].value(aggregates);
                }
                results.add(
                        new APLExplanationResult(qualityMetrics, curSet, aggregates, metrics)
                );
            }
        }
        return results;
    }

    /**
     * Check if all subsets of an order-3 candidate are order-2 candidates.
     * @param o2Candidates All candidates of order 2 with minimum support.
     * @param curCandidate An order-3 candidate
     * @return Boolean
     */
    private static boolean validateCandidate(IntSet curCandidate,
                                      HashSet<IntSet> o2Candidates) {
        IntSet subPair;
        subPair = new IntSetAsArray(
                curCandidate.getFirst(),
                curCandidate.getSecond());
        if (o2Candidates.contains(subPair)) {
            subPair = new IntSetAsArray(
                    curCandidate.getSecond(),
                    curCandidate.getThird());
            if (o2Candidates.contains(subPair)) {
                subPair = new IntSetAsArray(
                        curCandidate.getFirst(),
                        curCandidate.getThird());
                if (o2Candidates.contains(subPair)) {
                    return true;
                }
            }
        }
        return false;
    }
}
