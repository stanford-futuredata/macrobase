package edu.stanford.futuredata.macrobase.analysis.summary.aplinearDistributed;

import edu.stanford.futuredata.macrobase.analysis.summary.util.*;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.QualityMetric;
import edu.stanford.futuredata.macrobase.util.MacrobaseInternalError;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLExplanationResult;
import org.apache.avro.generic.GenericData;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Class for handling the generic, algorithmic aspects of apriori explanation.
 * This class assumes that subgroups posses "aggregates" such as count and outlier_count
 * which can be combined additively. Then, we use APriori to find the subgroups which
 * are the most interesting as defined by "quality metrics" on these aggregates.
 */
public class APrioriLinearDistributed {
    private Logger log = LoggerFactory.getLogger("APLSummarizerDistributed");

    // **Parameters**
    private QualityMetric[] qualityMetrics;
    private double[] thresholds;
    JavaSparkContext sparkContext;

    // **Cached values**
    // Singleton viable sets for quick lookup
    private boolean[] singleNextArray;
    // Sets that have high enough support but not high qualityMetrics, need to be explored
    private HashMap<Integer, HashSet<IntSet>> setNext;
    // Aggregate values for all of the sets we saved
    private HashMap<Integer, Map<IntSet, double []>> savedAggregates;

    public APrioriLinearDistributed(
            List<QualityMetric> qualityMetrics,
            List<Double> thresholds,
            JavaSparkContext sparkContext
    ) {
        this.qualityMetrics = qualityMetrics.toArray(new QualityMetric[0]);
        this.thresholds = new double[thresholds.size()];
        for (int i = 0; i < thresholds.size(); i++) {
            this.thresholds[i] = thresholds.get(i);
        }
        this.setNext = new HashMap<>(3);
        this.savedAggregates = new HashMap<>(3);
        this.sparkContext = sparkContext;
    }

    public List<APLExplanationResult> explain(
            final int[][] attributes,
            double[][] aggregateColumns,
            int cardinality,
            int numThreads
    ) {
        final int numAggregates = aggregateColumns.length;
        final int numRows = aggregateColumns[0].length;
        final int numColumns = attributes[0].length;

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

        // Shard the dataset by rows for the threads, but store it by column for fast processing
        final List<int[][]> byThreadAttributesTranspose = new ArrayList<>(numThreads);
        for (int threadNum = 0; threadNum < numThreads; threadNum++) {
            final int startIndex = (numRows * threadNum) / numThreads;
            final int endIndex = (numRows * (threadNum + 1)) / numThreads;
            int[][] thisPartition = new int[numColumns][(numRows + numThreads)/numThreads];
            for(int i = 0; i < numColumns; i++) {
                for (int j = startIndex; j < endIndex; j++) {
                    thisPartition[i][j - startIndex] = attributes[j][i];
                }
            }
            byThreadAttributesTranspose.add(thisPartition);
        }

        // Quality metrics are initialized with global aggregates to
        // allow them to determine the appropriate relative thresholds
        double[] globalAggregates = new double[numAggregates];
        for (int j = 0; j < numAggregates; j++) {
            globalAggregates[j] = 0;
            double[] curColumn = aggregateColumns[j];
            for (int i = 0; i < numRows; i++) {
                globalAggregates[j] += curColumn[i];
            }
        }
        for (QualityMetric q : qualityMetrics) {
            q.initialize(globalAggregates);
        }

        // Shared and row store for more convenient access
        final List<double[][]> aRows = new ArrayList<>(numThreads);
        for(int threadNum = 0; threadNum < numThreads; threadNum++) {
            final int startIndex = (numRows * threadNum) / numThreads;
            final int endIndex = (numRows * (threadNum + 1)) / numThreads;
            double[][] thisPartition = new double[endIndex - startIndex][numAggregates];
            for (int i = startIndex; i < endIndex; i++) {
                for (int j = 0; j < numAggregates; j++) {
                    thisPartition[i - startIndex][j] = aggregateColumns[j][i];
                }
            }
            aRows.add(thisPartition);
        }

        final List<TupleForSpark> sparkTuples = new ArrayList<>(numThreads);
        for(int i = 0; i < numThreads; i++) {
            sparkTuples.add(new TupleForSpark(byThreadAttributesTranspose.get(i), aRows.get(i)));
        }
        JavaRDD<TupleForSpark> tupleRDD = sparkContext.parallelize(sparkTuples, numThreads);

        for (int curOrder = 1; curOrder <= 3; curOrder++) {
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
                final FastFixedHashTable thisThreadSetAggregates = threadSetAggregates.get(threadNum);
                // Do candidate generation in a lambda.
                Runnable APrioriLinearRunnable = () -> {
                    IntSet curCandidate;
                    if (!useIntSetAsArray)
                        curCandidate = new IntSetAsLong(0);
                    else
                        curCandidate = new IntSetAsArray(0);
                    double[][] aRowsForThread = aRows.get(curThreadNum);
                    if (curOrderFinal == 1) {
                        for (int colNum = 0; colNum < numColumns; colNum++) {
                            int[] curColumnAttributes = byThreadAttributesTranspose.get(curThreadNum)[colNum];
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
                                double[] candidateVal = thisThreadSetAggregates.get(curCandidate);
                                if (candidateVal == null) {
                                    thisThreadSetAggregates.put(curCandidate,
                                            Arrays.copyOf(aRowsForThread[rowNum], numAggregates));
                                } else {
                                    for (int a = 0; a < numAggregates; a++) {
                                        candidateVal[a] += aRowsForThread[rowNum][a];
                                    }
                                }
                            }
                        }
                    } else if (curOrderFinal == 2) {
                        for (int colNumOne = 0; colNumOne < numColumns; colNumOne++) {
                            int[] curColumnOneAttributes = byThreadAttributesTranspose.get(curThreadNum)[colNumOne];
                            for (int colNumTwo = colNumOne + 1; colNumTwo < numColumns; colNumTwo++) {
                                int[] curColumnTwoAttributes = byThreadAttributesTranspose.get(curThreadNum)[colNumTwo];
                                for (int rowNum = 0; rowNum < aRowsForThread.length; rowNum++) {
                                    // Only examine a pair if both its members have minimum support.
                                    if (curColumnOneAttributes[rowNum] == AttributeEncoder.noSupport
                                            || curColumnTwoAttributes[rowNum] == AttributeEncoder.noSupport
                                            || !singleNextArray[curColumnOneAttributes[rowNum]]
                                            || !singleNextArray[curColumnTwoAttributes[rowNum]])
                                        continue;
                                    // Cascade to arrays if necessary, but otherwise pack attributes into longs.
                                    if (useIntSetAsArray) {
                                        curCandidate = new IntSetAsArray(curColumnOneAttributes[rowNum],
                                                curColumnTwoAttributes[rowNum]);
                                    } else {
                                        ((IntSetAsLong) curCandidate).value = IntSetAsLong.twoIntToLong(curColumnOneAttributes[rowNum],
                                                curColumnTwoAttributes[rowNum]);
                                    }
                                    double[] candidateVal = thisThreadSetAggregates.get(curCandidate);
                                    if (candidateVal == null) {
                                        thisThreadSetAggregates.put(curCandidate,
                                                Arrays.copyOf(aRowsForThread[rowNum], numAggregates));
                                    } else {
                                        for (int a = 0; a < numAggregates; a++) {
                                            candidateVal[a] += aRowsForThread[rowNum][a];
                                        }
                                    }
                                }
                            }
                        }
                    } else if (curOrderFinal == 3) {
                        for (int colNumOne = 0; colNumOne < numColumns; colNumOne++) {
                            int[] curColumnOneAttributes = byThreadAttributesTranspose.get(curThreadNum)[colNumOne % numColumns];
                            for (int colNumTwo = colNumOne + 1; colNumTwo < numColumns; colNumTwo++) {
                                int[] curColumnTwoAttributes = byThreadAttributesTranspose.get(curThreadNum)[colNumTwo % numColumns];
                                for (int colnumThree = colNumTwo + 1; colnumThree < numColumns; colnumThree++) {
                                    int[] curColumnThreeAttributes = byThreadAttributesTranspose.get(curThreadNum)[colnumThree % numColumns];
                                    for (int rowNum = 0; rowNum < aRowsForThread.length; rowNum++) {
                                        // Only construct a triple if all its singleton members have minimum support.
                                        if (curColumnOneAttributes[rowNum] == AttributeEncoder.noSupport
                                                || curColumnTwoAttributes[rowNum] == AttributeEncoder.noSupport
                                                || curColumnThreeAttributes[rowNum] == AttributeEncoder.noSupport
                                                || !singleNextArray[curColumnThreeAttributes[rowNum]]
                                                || !singleNextArray[curColumnOneAttributes[rowNum]]
                                                || !singleNextArray[curColumnTwoAttributes[rowNum]])
                                            continue;
                                        // Cascade to arrays if necessary, but otherwise pack attributes into longs.
                                        if (useIntSetAsArray) {
                                            curCandidate = new IntSetAsArray(
                                                    curColumnOneAttributes[rowNum],
                                                    curColumnTwoAttributes[rowNum],
                                                    curColumnThreeAttributes[rowNum]);
                                        } else {
                                            ((IntSetAsLong) curCandidate).value = IntSetAsLong.threeIntToLong(
                                                    curColumnOneAttributes[rowNum],
                                                    curColumnTwoAttributes[rowNum],
                                                    curColumnThreeAttributes[rowNum]);
                                        }
                                        double[] candidateVal = thisThreadSetAggregates.get(curCandidate);
                                        if (candidateVal == null) {
                                            thisThreadSetAggregates.put(curCandidate,
                                                    Arrays.copyOf(aRowsForThread[rowNum], numAggregates));
                                        } else {
                                            for (int a = 0; a < numAggregates; a++) {
                                                candidateVal[a] += aRowsForThread[rowNum][a];
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        throw new MacrobaseInternalError("High Order not supported");
                    }
                    log.info("Time spent in Thread {} in order {}:  {}",
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
                                candidateVal[a] += curCandidateValue[a];
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
                                candidateVal[a] += curCandidateValue[a];
                            }
                        }
                    }
                }
            }

            // Prune all the collected aggregates
            HashSet<IntSet> curOrderNext = new HashSet<>();
            HashSet<IntSet> curOrderSaved = new HashSet<>();
            for (IntSet curCandidate: setAggregates.keySet()) {
                if (curOrder == 1 && curCandidate.getFirst() == AttributeEncoder.noSupport) {
                    continue;
                }
                double[] curAggregates = setAggregates.get(curCandidate);
                boolean canPassThreshold = true;
                boolean isPastThreshold = true;
                for (int i = 0; i < qualityMetrics.length; i++) {
                    QualityMetric q = qualityMetrics[i];
                    double t = thresholds[i];
                    canPassThreshold &= q.maxSubgroupValue(curAggregates) >= t;
                    isPastThreshold &= q.value(curAggregates) >= t;
                }
                if (canPassThreshold) {
                    // if a set is already past the threshold on all metrics,
                    // save it and no need for further exploration
                    if (isPastThreshold && !(curOrder == 3 && !validateCandidate(curCandidate, setNext.get(2)))) {
                        curOrderSaved.add(curCandidate);
                    }
                    else {
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
                singleNextArray = new boolean[cardinality];
                for (IntSet i : curOrderNext) {
                    singleNextArray[i.getFirst()] = true;
                }
            }
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
    private boolean validateCandidate(IntSet curCandidate,
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

class TupleForSpark {
    public final int[][] attributes;
    public final double[][] aRows;

    TupleForSpark(int[][] attributes, double[][] aRows) {
        this.attributes = attributes;
        this.aRows = aRows;
    }
}