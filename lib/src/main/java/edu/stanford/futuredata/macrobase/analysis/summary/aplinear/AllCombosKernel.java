package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import static edu.stanford.futuredata.macrobase.analysis.summary.aplinear.BitmapHelperFunctions.allThreeBitmap;
import static edu.stanford.futuredata.macrobase.analysis.summary.aplinear.BitmapHelperFunctions.allThreeNormal;
import static edu.stanford.futuredata.macrobase.analysis.summary.aplinear.BitmapHelperFunctions.allTwoBitmap;
import static edu.stanford.futuredata.macrobase.analysis.summary.aplinear.BitmapHelperFunctions.allTwoNormal;
import static edu.stanford.futuredata.macrobase.analysis.summary.aplinear.BitmapHelperFunctions.updateAggregates;

import edu.stanford.futuredata.macrobase.analysis.summary.util.*;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.AggregationOp;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.QualityMetric;
import edu.stanford.futuredata.macrobase.util.MacroBaseInternalError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;

public class AllCombosKernel extends APrioriLinear {

    private Logger log = LoggerFactory.getLogger("AllCombosKernel");

    public AllCombosKernel(List<QualityMetric> qualityMetrics, List<List<Double>> thresholds,
        AttributeEncoder encoder) {
        // TODO support list of thresholds
        super(qualityMetrics, thresholds, encoder);
    }

    public List<APLExplanationResult> explain(
        final int[][] attributes,
        double[][] aggregateColumns,
        double[][] globalAggregateCols,
        AggregationOp[] aggregationOps,
        int cardinality,
        final int maxOrder,
        int numThreads,
        HashMap<Integer, ModBitSet>[][] bitmap,
        ArrayList<Integer>[] outlierList,
        int[] colCardinalities,
        int bitmapRatioThreshold
    ) {
        final long beginTime = System.currentTimeMillis();
        final int numAggregates = aggregateColumns.length;
        final int numRows = aggregateColumns[0].length;
        final int numColumns = attributes[0].length;

        // Singleton viable sets for quick lookup
        boolean[] singleNextArray = new boolean[cardinality];
        final HashMap<Integer, Map<IntSet, double []>> savedAggregates= new HashMap<>(3);

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
        log.info("NumThreads: {}", numThreads);
        // Shard the dataset by rows for the threads, but store it by column for fast processing
        final int[][][] byThreadAttributesTranspose =
            new int[numThreads][numColumns][(numRows + numThreads)/numThreads];
        final HashMap<Integer, ModBitSet>[][][] byThreadBitmap = new HashMap[numThreads][numColumns][2];
        for (int i = 0; i < numThreads; i++)
            for (int j = 0; j < numColumns; j++)
                for (int k = 0; k < 2; k++)
                    byThreadBitmap[i][j][k] = new HashMap<>();
        for (int threadNum = 0; threadNum < numThreads; threadNum++) {
            final int startIndex = (numRows * threadNum) / numThreads;
            final int endIndex = (numRows * (threadNum + 1)) / numThreads;
            for(int i = 0; i < numColumns; i++) {
                for (int j = startIndex; j < endIndex; j++) {
                    byThreadAttributesTranspose[threadNum][i][j - startIndex] = attributes[j][i];
                }
                if (colCardinalities[i] < AttributeEncoder.cardinalityThreshold) {
                    for (int j = 0; j < 2; j++) {
                        for (HashMap.Entry<Integer, ModBitSet> entry : bitmap[i][j].entrySet()) {
                            ModBitSet rr = entry.getValue().get(startIndex, endIndex);
                            if (rr.cardinality() > 0) {
                                byThreadBitmap[threadNum][i][j].put(entry.getKey(), rr);
                            }
                        }
                    }
                }
            }
        }

        // Quality metrics are initialized with global aggregates to
        // allow them to determine the appropriate relative thresholds
        double[] globalAggregates = new double[numAggregates];
        for (int j = 0; j < numAggregates; j++) {
            AggregationOp curOp = aggregationOps[j];
            globalAggregates[j] = curOp.initValue();
            double[] curColumn = globalAggregateCols[j];
            for (int i = 0; i < curColumn.length; i++) {
                globalAggregates[j] = curOp.combine(globalAggregates[j], curColumn[i]);
            }
        }
        for (QualityMetric q : qualityMetrics) {
            q.initialize(globalAggregates);
        }

        // Row store for more convenient access
        final double[][] aRows = new double[numRows][numAggregates];
        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < numAggregates; j++) {
                aRows[i][j] = aggregateColumns[j][i];
            }
        }
        for (int curOrder = 1; curOrder <= maxOrder; curOrder++) {
            long startTime = System.currentTimeMillis();
            final int curOrderFinal = curOrder;
            // Initialize per-thread hashmaps.
            final ArrayList<FastFixedHashTable> threadSetAggregates = new ArrayList<>(numThreads);
            for (int i = 0; i < numThreads; i++) {
                threadSetAggregates.add(new FastFixedHashTable(cardinality, numAggregates, useIntSetAsArray));
            }
            // Shard the dataset by row into threads and generate candidates.
            final CountDownLatch doneSignal = new CountDownLatch(numThreads);
            for (int threadNum = 0; threadNum < numThreads; threadNum++) {
                final int curThreadNum = threadNum;
                final int startIndex = (numRows * threadNum) / numThreads;
                final int endIndex = (numRows * (threadNum + 1)) / numThreads;
                final FastFixedHashTable thisThreadSetAggregates = threadSetAggregates.get(threadNum);
                // Do candidate generation in a lambda.
                Runnable APrioriLinearRunnable = () -> {
                    IntSet curCandidate;
                    if (!useIntSetAsArray)
                        curCandidate = new IntSetAsLong(0);
                    else
                        curCandidate = new IntSetAsArray(0);
                    if (curOrderFinal == 1) {
                        for (int colNum = 0; colNum < numColumns; colNum++) {
                            if (colCardinalities[colNum] < AttributeEncoder.cardinalityThreshold) {
                                for (Integer curOutlierCandidate : outlierList[colNum]) {
                                    // Require that all order-one candidates have minimum support.
                                    if (curOutlierCandidate == AttributeEncoder.noSupport)
                                        continue;
                                    int outlierCount = 0, inlierCount = 0;
                                    if (byThreadBitmap[curThreadNum][colNum][1].containsKey(curOutlierCandidate))
                                        outlierCount = byThreadBitmap[curThreadNum][colNum][1].get(curOutlierCandidate).cardinality();
                                    if (byThreadBitmap[curThreadNum][colNum][0].containsKey(curOutlierCandidate))
                                        inlierCount = byThreadBitmap[curThreadNum][colNum][0].get(curOutlierCandidate).cardinality();
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
                                int[] curColumnAttributes = byThreadAttributesTranspose[curThreadNum][colNum];
                                for (int rowNum = startIndex; rowNum < endIndex; rowNum++) {
                                    // Require that all order-one candidates have minimum support.
                                    if (curColumnAttributes[rowNum - startIndex] == AttributeEncoder.noSupport)
                                        continue;
                                    // Cascade to arrays if necessary, but otherwise pack attributes into longs.
                                    if (useIntSetAsArray) {
                                        curCandidate = new IntSetAsArray(curColumnAttributes[rowNum - startIndex]);
                                    } else {
                                        ((IntSetAsLong) curCandidate).value = curColumnAttributes[rowNum - startIndex];
                                    }
                                    updateAggregates(thisThreadSetAggregates, curCandidate, aggregationOps,
                                        aRows[rowNum], numAggregates);
                                }
                            }
                        }
                    } else if (curOrderFinal == 2) {
                        for (int colNumOne = 0; colNumOne < numColumns; colNumOne++) {
                            int[] curColumnOneAttributes = byThreadAttributesTranspose[curThreadNum][colNumOne];
                            for (int colNumTwo = colNumOne + 1; colNumTwo < numColumns; colNumTwo++) {
                                int[] curColumnTwoAttributes = byThreadAttributesTranspose[curThreadNum][colNumTwo];
                                if (colCardinalities[colNumOne] < AttributeEncoder.cardinalityThreshold &&
                                    colCardinalities[colNumOne] < AttributeEncoder.cardinalityThreshold &&
                                    colCardinalities[colNumOne] * colCardinalities[colNumTwo] < bitmapRatioThreshold) {
                                    // Bitmap-Bitmap
                                    allTwoBitmap(thisThreadSetAggregates, outlierList, aggregationOps, singleNextArray,
                                        byThreadBitmap[curThreadNum], colNumOne, colNumTwo, useIntSetAsArray,
                                        curCandidate, numAggregates);
                                }  else {
                                    // Normal-Normal
                                    allTwoNormal(thisThreadSetAggregates, curColumnOneAttributes,
                                        curColumnTwoAttributes, aggregationOps, singleNextArray,
                                        startIndex, endIndex, useIntSetAsArray, curCandidate, aRows,
                                        numAggregates);
                                }
                            }
                        }
                    } else if (curOrderFinal == 3) {
                        // order-3 case
                        for (int colNumOne = 0; colNumOne < numColumns; colNumOne++) {
                            int[] curColumnOneAttributes = byThreadAttributesTranspose[curThreadNum][colNumOne % numColumns];
                            for (int colNumTwo = colNumOne + 1; colNumTwo < numColumns; colNumTwo++) {
                                int[] curColumnTwoAttributes = byThreadAttributesTranspose[curThreadNum][colNumTwo % numColumns];
                                for (int colNumThree = colNumTwo + 1; colNumThree < numColumns; colNumThree++) {
                                    int[] curColumnThreeAttributes = byThreadAttributesTranspose[curThreadNum][colNumThree % numColumns];
                                    if (colCardinalities[colNumOne] < AttributeEncoder.cardinalityThreshold &&
                                        colCardinalities[colNumOne] < AttributeEncoder.cardinalityThreshold &&
                                        colCardinalities[colNumThree] < AttributeEncoder.cardinalityThreshold &&
                                        colCardinalities[colNumOne] * colCardinalities[colNumTwo] *
                                            colCardinalities[colNumThree] < bitmapRatioThreshold) {
                                        // all 3 cols are bitmaps
                                        allThreeBitmap(thisThreadSetAggregates, outlierList, aggregationOps,
                                            singleNextArray, byThreadBitmap[curThreadNum],
                                            colNumOne, colNumTwo, colNumThree, useIntSetAsArray, curCandidate,
                                            numAggregates);

                                    } else {
                                        // all three are normal
                                        allThreeNormal(thisThreadSetAggregates, curColumnOneAttributes,
                                            curColumnTwoAttributes, curColumnThreeAttributes,
                                            aggregationOps, singleNextArray, startIndex, endIndex,
                                            useIntSetAsArray, curCandidate, aRows, numAggregates);
                                    }
                                }
                            }
                        }
                    } else {
                        throw new MacroBaseInternalError("High Order not supported");
                    }
                    log.info("Time spent in Thread {} in order {}:  {} ms",
                        curThreadNum, curOrderFinal, System.currentTimeMillis() - startTime);
                    doneSignal.countDown();
                };
                // Run numThreads lambdas in separate threads
                Thread APrioriLinearThread = new Thread(APrioriLinearRunnable);
                APrioriLinearThread.start();
            }
            // Wait for all threads to finish running.
            try {
                doneSignal.await();
            } catch (InterruptedException ex) {ex.printStackTrace();}


            Map<IntSet, double []> setAggregates = new HashMap<>();
            // Collect the aggregates stored in the per-thread HashMaps.
            for (FastFixedHashTable set : threadSetAggregates) {
                if (useIntSetAsArray) {
                    for (IntSet curCandidateKey : set.keySet()) {
                        double[] curCandidateValue = set.get(curCandidateKey);
                        double[] candidateVal = setAggregates.get(curCandidateKey);
                        if (candidateVal == null) {
                            setAggregates.put(curCandidateKey, Arrays.copyOf(curCandidateValue, numAggregates));
                        } else {
                            for (int a = 0; a < numAggregates; a++) {
                                AggregationOp curOp = aggregationOps[a];
                                candidateVal[a] = curOp.combine(candidateVal[a], curCandidateValue[a]);
                            }
                        }
                    }
                } else {
                    for (long curCandidateKeyLong : set.keySetLong()) {
                        IntSetAsLong curCandidateKeyIntSetAsLong = new IntSetAsLong(curCandidateKeyLong);
                        IntSet curCandidateKey = new IntSetAsArray(curCandidateKeyIntSetAsLong);
                        double[] curCandidateValue = set.get(curCandidateKeyIntSetAsLong);
                        double[] candidateVal = setAggregates.get(curCandidateKey);
                        if (candidateVal == null) {
                            setAggregates.put(curCandidateKey, Arrays.copyOf(curCandidateValue, numAggregates));
                        } else {
                            for (int a = 0; a < numAggregates; a++) {
                                AggregationOp curOp = aggregationOps[a];
                                candidateVal[a] = curOp.combine(candidateVal[a], curCandidateValue[a]);
                            }
                        }
                    }
                }
            }

            HashSet<IntSet> curOrderNext = new HashSet<>();
            HashSet<IntSet> curOrderSaved = new HashSet<>();
            for (IntSet curCandidate: setAggregates.keySet()) {
                // NOTE: this assumes that outlier count is the first aggregate in the array
                // TODO: come up with a cleaner solution
                if (setAggregates.get(curCandidate)[0] == 0.0) {
                    continue;
                }
                curOrderSaved.add(curCandidate);
                curOrderNext.add(curCandidate);
            }

            // Save aggregates that pass all qualityMetrics to return later, store aggregates
            // that have minimum support for higher-order exploration.
            Map<IntSet, double []> curSavedAggregates = new HashMap<>(curOrderSaved.size());
            for (IntSet curSaved : curOrderSaved) {
                curSavedAggregates.put(curSaved, setAggregates.get(curSaved));
            }
            savedAggregates.put(curOrder, curSavedAggregates);
            if (curOrder == 1) {
                for (IntSet i : curOrderNext) {
                    singleNextArray[i.getFirst()] = true;
                }
            }
        }
        log.info("Time spent in allCombos:  {} ms", System.currentTimeMillis() - beginTime);

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
}
